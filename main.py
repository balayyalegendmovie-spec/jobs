import os
import json
import asyncio
import aiohttp
import gspread
import random
import re
import feedparser
from datetime import datetime
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from ddgs import DDGS

# ==========================================
# GLOBAL CONFIG
# ==========================================

MAX_JOBS_PER_RUN = 30
SCRAPE_CONCURRENCY = 5
GLOBAL_TIMEOUT = 900  # 15 minutes
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

SHEET_NAME = os.getenv("SHEET_NAME", "Job_Search_Master")

QUERIES = [
    'site:instahyre.com "Cyber Security" Bangalore',
    'site:cutshort.io "Cyber Security" India',
    'site:hirist.tech "Cyber Security" India',
    'site:naukri.com "Cyber Security" Bangalore not:senior',
    'site:wellfound.com/jobs "Cyber Security" India'
]

# ==========================================
# SAFE LOGGER
# ==========================================

def log(msg):
    print(msg, flush=True)

# ==========================================
# SAFE GOOGLE SHEETS CONNECTION
# ==========================================

async def setup_google_sheet():
    def connect():
        creds_json = os.getenv("GOOGLE_SHEET_CREDS")
        if not creds_json:
            raise Exception("Missing GOOGLE_SHEET_CREDS")

        creds = Credentials.from_service_account_info(
            json.loads(creds_json),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )

        client = gspread.authorize(creds)
        return client.open(SHEET_NAME).sheet1

    try:
        return await asyncio.wait_for(asyncio.to_thread(connect), timeout=30)
    except Exception as e:
        log(f"❌ Google Sheets Error: {e}")
        return None

# ==========================================
# SAFE SEARCH
# ==========================================

async def search_google(session, query):
    key = os.getenv("SAPI")
    cx = os.getenv("CXAPI")

    if not key or not cx:
        return []

    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": key, "cx": cx, "q": query, "dateRestrict": "m1"}

    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("items", [])
            else:
                log(f"⚠️ Google API status {resp.status}")
    except Exception as e:
        log(f"⚠️ Google Search Error: {e}")

    return []

async def search_ddg(query):
    def run():
        try:
            results = []
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=5, timelimit="m"):
                    results.append({
                        "title": r.get("title"),
                        "link": r.get("href"),
                        "snippet": r.get("body", "")
                    })
            return results
        except Exception as e:
            log(f"⚠️ DDG Error: {e}")
            return []

    try:
        return await asyncio.wait_for(asyncio.to_thread(run), timeout=30)
    except:
        log("⚠️ DDG Timeout")
        return []

async def fetch_rss():
    try:
        feed = feedparser.parse(
            "https://jobicy.com/?feed=job_feed&job_categories=security-engineer&job_types=remote"
        )
        return [{"title": e.title, "link": e.link, "snippet": e.summary[:200]}
                for e in feed.entries]
    except Exception as e:
        log(f"⚠️ RSS Error: {e}")
        return []

# ==========================================
# SAFE SCRAPER
# ==========================================

async def scrape_page(session, url, fallback, sem):
    async with sem:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    for tag in soup(["script", "style", "nav", "footer"]):
                        tag.extract()
                    text = soup.get_text(" ", strip=True)
                    return text[:3000]
        except Exception:
            return fallback
    return fallback

# ==========================================
# SAFE TELEGRAM
# ==========================================

async def send_telegram(session, message):
    token = os.getenv("TOK")
    chat_id = os.getenv("ID")

    if not token or not chat_id:
        return

    try:
        await session.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message},
            timeout=REQUEST_TIMEOUT
        )
    except Exception as e:
        log(f"⚠️ Telegram Error: {e}")

# ==========================================
# MAIN
# ==========================================

async def main():

    log("🚀 Bot Started")

    # --- Google Sheets ---
    log("🔐 Connecting to Google Sheets...")
    sheet = await setup_google_sheet()
    if sheet:
        log("✅ Google Sheets Connected")
    else:
        log("⚠️ Running without Google Sheets")

    existing_links = set()
    if sheet:
        try:
            existing_links = set(sheet.col_values(5)[1:])
        except Exception as e:
            log(f"⚠️ Failed loading existing links: {e}")

    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    headers = {"User-Agent": "Mozilla/5.0"}

    async with aiohttp.ClientSession(headers=headers) as session:

        # --- SEARCH ---
        log("🔍 Starting Searches")

        google_tasks = [search_google(session, q) for q in QUERIES]
        ddg_tasks = [search_ddg(q) for q in QUERIES]

        google_results = await asyncio.gather(*google_tasks)
        ddg_results = await asyncio.gather(*ddg_tasks)
        rss_results = await fetch_rss()

        all_results = google_results + ddg_results + [rss_results]

        jobs = []

        for batch in all_results:
            for item in batch:
                link = (item.get("link") or "").split("?")[0]
                title = item.get("title") or ""

                if not link or link in existing_links:
                    continue

                if any(x in title.lower() for x in ["senior", "manager", "lead"]):
                    continue

                jobs.append({
                    "title": title,
                    "link": link,
                    "snippet": item.get("snippet", "")
                })

        jobs = jobs[:MAX_JOBS_PER_RUN]

        log(f"✅ {len(jobs)} jobs after filtering")

        # --- SCRAPE ---
        log("🌐 Scraping pages...")

        scrape_tasks = [
            scrape_page(session, j["link"], j["snippet"], semaphore)
            for j in jobs
        ]

        full_texts = await asyncio.gather(*scrape_tasks)

        for i, text in enumerate(full_texts):
            jobs[i]["full_text"] = text
            log(f"✅ Scraped {i+1}/{len(full_texts)}")

        # --- SAVE ---
        if sheet and jobs:
    try:
        rows = []

        for j in jobs:
            row = [
                "New",                 # Status
                "Not AI Filtered",     # AI Verdict
                j["title"],            # Role
                "Unknown",             # Company
                j["link"],             # Link
                str(datetime.now()),   # Date
                "0%",                  # Match %
                "0%",                  # Suitability %
                ""                     # Cover Letter
            ]
            rows.append(row)

        await asyncio.to_thread(sheet.append_rows, rows, value_input_option="USER_ENTERED")

        log(f"💾 Saved {len(rows)} jobs to sheet")

    except Exception as e:
        log(f"⚠️ Sheet Write Error: {e}")

        # --- TELEGRAM ---
        for job in jobs[:5]:
            await send_telegram(
                session,
                f"🤖 JOB ALERT\n\n{job['title']}\n{job['link']}"
            )

    log("✅ Run Completed Successfully")

# ==========================================
# ENTRY WITH HARD GLOBAL TIMEOUT
# ==========================================

if __name__ == "__main__":
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=GLOBAL_TIMEOUT))
    except asyncio.TimeoutError:
        log("⏰ Global timeout reached. Exiting safely.")
    except Exception as e:
        log(f"❌ Fatal Error: {e}")
