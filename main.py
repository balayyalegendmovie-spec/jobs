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
# 1. CONFIGURATION
# ==========================================

MAX_JOBS_PER_RUN = 40
SCRAPE_CONCURRENCY = 10
GLOBAL_TIMEOUT = 900  # 15 minutes

SEARCH_POOLS = [
    {"key": os.getenv("SAPI"), "cx": os.getenv("CXAPI")}
]

GEMINI_KEYS = [os.getenv(f"GAPI{i}") for i in range(1, 7) if os.getenv(f"GAPI{i}")]
GROQ_KEYS = [os.getenv(f"GRAPI{i}") for i in range(1, 7) if os.getenv(f"GRAPI{i}")]

SHEET_NAME = os.getenv("SHEET_NAME", "Jobs")
TELEGRAM_TOKEN = os.getenv("TOK")
CHAT_ID = os.getenv("ID")
USER_PROFILE = os.getenv("USER_PROFILE_INFO", "")

QUERIES = [
    'site:instahyre.com ("Cyber Security" OR "Security Analyst") Bangalore',
    'site:cutshort.io ("Cyber Security" OR "Security Analyst") India',
    'site:hirist.tech ("Cyber Security") India',
    'site:naukri.com ("Cyber Security") Bangalore not:senior',
    'site:foundit.in ("Cyber Security") Bangalore',
    'site:wellfound.com/jobs ("Cyber Security") India',
    'site:weworkremotely.com ("Security") not:senior'
]

# ==========================================
# 2. HELPERS
# ==========================================

def get_search_cred():
    return random.choice(SEARCH_POOLS) if SEARCH_POOLS and SEARCH_POOLS[0]['key'] else None

def get_gemini_key():
    return random.choice(GEMINI_KEYS) if GEMINI_KEYS else None

def get_groq_key():
    return random.choice(GROQ_KEYS) if GROQ_KEYS else None

def safe_parse_json(text):
    try:
        return json.loads(text)
    except:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except:
                pass
    return {}

# ==========================================
# 3. SEARCH FUNCTIONS
# ==========================================

async def search_ddg(query):
    def sync_search():
        try:
            results = []
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=10, timelimit="m"):
                    results.append({
                        "title": r.get("title", ""),
                        "link": r.get("href", ""),
                        "snippet": r.get("body", "")
                    })
            return results
        except:
            return []
    return await asyncio.to_thread(sync_search)

async def search_google(session, query):
    cred = get_search_cred()
    if not cred:
        return []

    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": cred['key'],
        "cx": cred['cx'],
        "q": query,
        "dateRestrict": "m1"
    }

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return (await resp.json()).get("items", [])
    except:
        pass
    return []

async def fetch_jobicy_rss():
    try:
        feed = feedparser.parse(
            "https://jobicy.com/?feed=job_feed&job_categories=security-engineer&job_types=remote"
        )
        jobs = []
        for entry in feed.entries:
            if any(x in entry.title.lower() for x in ["senior", "lead", "manager"]):
                continue
            jobs.append({
                "title": entry.title,
                "link": entry.link,
                "snippet": entry.summary[:200]
            })
        return jobs
    except:
        return []

# ==========================================
# 4. SCRAPING (LIMITED CONCURRENCY)
# ==========================================

async def fetch_full_text(session, url, fallback, semaphore):
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    for tag in soup(["script", "style", "nav", "footer"]):
                        tag.extract()
                    text = soup.get_text(" ", strip=True)
                    return text[:3000] if len(text) > 100 else fallback
        except:
            pass
        return fallback

# ==========================================
# 5. AI CALLS (SAFE TIMEOUT)
# ==========================================

async def call_gemini(session, prompt):
    key = get_gemini_key()
    if not key:
        return []

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                text = (await resp.json())['candidates'][0]['content']['parts'][0]['text']
                return safe_parse_json(text).get("matches", [])
    except:
        pass
    return []

# ==========================================
# 6. MAIN
# ==========================================

async def main():
    print("🚀 Bot Started")

    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GOOGLE_SHEET_CREDS']),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    client = gspread.authorize(creds)
    sheet = client.open("Job_Search_Master").sheet1

    existing_links = set(sheet.col_values(5)[1:])
    new_rows = []

    headers = {"User-Agent": "Mozilla/5.0"}
    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async with aiohttp.ClientSession(headers=headers) as session:

        print("🔍 Searching...")
        tasks = [search_google(session, q) for q in QUERIES]
        tasks += [search_ddg(q) for q in QUERIES]
        tasks.append(fetch_jobicy_rss())

        results = await asyncio.gather(*tasks)

        jobs_buffer = []
        for batch in results:
            for item in batch:
                link = item.get("link", "").split("?")[0]
                title = item.get("title", "")

                if not link or link in existing_links:
                    continue

                if any(x in title.lower() for x in ["senior", "manager", "lead"]):
                    continue

                jobs_buffer.append({
                    "title": title,
                    "link": link,
                    "snippet": item.get("snippet", "")
                })

                existing_links.add(link)

        jobs_buffer = jobs_buffer[:MAX_JOBS_PER_RUN]

        print(f"✅ Processing {len(jobs_buffer)} jobs")

        scrape_tasks = [
            fetch_full_text(session, j["link"], j["snippet"], semaphore)
            for j in jobs_buffer
        ]

        full_texts = await asyncio.gather(*scrape_tasks)

        for i, text in enumerate(full_texts):
            jobs_buffer[i]["full_text"] = text

        for job in jobs_buffer:
            row = [
                "New",
                job["title"],
                job["link"],
                str(datetime.now())
            ]
            new_rows.append(row)

        if new_rows:
            sheet.append_rows(new_rows)
            print(f"💾 Saved {len(new_rows)} jobs")

    print("✅ Run completed safely")

# ==========================================
# 7. SAFE ENTRY POINT
# ==========================================

if __name__ == "__main__":
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=GLOBAL_TIMEOUT))
    except asyncio.TimeoutError:
        print("⏰ Script safely timed out (15 min limit).")
