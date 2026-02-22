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
from duckduckgo_search import DDGS

# ==========================================
# 1. CONFIGURATION (THE HYDRA 🐉)
# ==========================================

SEARCH_POOLS = [
    {"key": os.getenv("SAPI"), "cx": os.getenv("CXAPI")}
]

GEMINI_KEYS = [os.getenv(f"GAPI{i}") for i in range(1, 7) if os.getenv(f"GAPI{i}")]
GROQ_KEYS = [os.getenv(f"GRAPI{i}") for i in range(1, 7) if os.getenv(f"GRAPI{i}")]

SHEET_NAME = os.getenv("SHEET_NAME", "Jobs")
TELEGRAM_TOKEN = os.getenv("TOK")
CHAT_ID = os.getenv("ID")
USER_PROFILE = os.getenv("USER_PROFILE_INFO")

QUERIES = [
    'site:instahyre.com ("Cyber Security" OR "Security Analyst" OR "VAPT") (Bangalore OR Remote)',
    'site:cutshort.io ("Cyber Security" OR "Security Analyst") India',
    'site:hirist.tech ("Cyber Security" OR "Security") India',
    'site:naukri.com ("Cyber Security" OR "Security Analyst") Bangalore not:senior',
    'site:foundit.in ("Cyber Security" OR "Security Analyst") Bangalore',
    'site:linkedin.com/jobs/view ("Cyber Security" OR "Security Analyst") Bangalore',
    'site:wellfound.com/jobs ("Cyber Security") India',
    'site:hiring.cafe ("Security" OR "Cyber") "India"',
    'site:weworkremotely.com ("Security") not:senior',
    'site:greenhouse.io/embed/job ("Cyber Security") India',
    'site:jobs.lever.co ("Security") India'
]

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_search_cred(): return random.choice(SEARCH_POOLS) if SEARCH_POOLS and SEARCH_POOLS[0]['key'] else None
def get_gemini_key(): return random.choice(GEMINI_KEYS) if GEMINI_KEYS else None
def get_groq_key(): return random.choice(GROQ_KEYS) if GROQ_KEYS else None

def safe_parse_json(text):
    try: return json.loads(text)
    except:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try: return json.loads(match.group(0))
            except: pass
    return {}

# ==========================================
# 3. SCRAPING & SEARCH LOGIC
# ==========================================

async def fetch_full_text(session, url, fallback_snippet):
    try:
        async with session.get(url, timeout=7) as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')
                for script in soup(["script", "style", "nav", "footer"]):
                    script.extract()
                text = soup.get_text(separator=' ', strip=True)
                return text[:4000] if len(text) > 100 else fallback_snippet
    except: pass
    return fallback_snippet

async def search_ddg(query):
    def sync_search():
        try:
            results = []
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=15):
                    results.append({"title": r.get("title", ""), "link": r.get("href", ""), "snippet": r.get("body", "")})
            return results
        except Exception as e:
            print(f"⚠️ DDG Error: {e}", flush=True)
            return []
    
    # This runs the synchronous search in a background thread so it acts like async!
    return await asyncio.to_thread(sync_search)

async def search_google(session, query, start_page):
    cred = get_search_cred()
    if not cred: return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": cred['key'], "cx": cred['cx'], "q": query, "start": start_page}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                return (await resp.json()).get("items", [])
    except: pass
    return []

async def fetch_jobicy_rss():
    print("📡 Scanning Jobicy (Remote)...", flush=True)
    try:
        feed = feedparser.parse("https://jobicy.com/?feed=job_feed&job_categories=security-engineer&job_types=remote")
        jobs = []
        for entry in feed.entries:
            if any(x in entry.title.lower() for x in ["senior", "head", "lead"]): continue
            jobs.append({"title": entry.title, "link": entry.link, "snippet": entry.summary[:200]})
        return jobs
    except: return []

# ==========================================
# 4. AI FILTERING & GENERATION
# ==========================================

AI_PROMPT = f"""
User Resume:
{USER_PROFILE}

Task: Filter these jobs based on the resume. 
Return ONLY JSON format strictly like this: {{"matches": [{{"index": 0, "match_percent": 85, "suitability": 90}}]}}
Rules: 
1. REJECT Senior/Manager/Sales roles. 
2. ACCEPT Analyst/Intern/Fresher roles. 
3. 'match_percent' = How well the job skills align with the resume (0-100). 
4. 'suitability' = The chance they will hire a fresher/intern like this user (0-100).

Jobs:
"""

def get_cv_prompt(job_text):
    return f"User Resume:\n{USER_PROFILE}\n\nTask: Write a highly professional, confident 3-paragraph cold email/cover letter applying for this job. Use the user's specific projects and skills. Do NOT include placeholders. Output ONLY the email text.\n\nJob Details:\n{job_text}"

async def call_gemini(session, text, task="filter"):
    key = get_gemini_key()
    if not key: return None if task == "filter" else ""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={key}"
    payload = {"contents": [{"parts": [{"text": (AI_PROMPT + text) if task == "filter" else get_cv_prompt(text)}]}]}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                res_text = (await resp.json())['candidates'][0]['content']['parts'][0]['text']
                return safe_parse_json(res_text).get("matches", []) if task == "filter" else res_text.strip()
    except: pass
    return None if task == "filter" else ""

async def call_groq(session, text, task="filter"):
    key = get_groq_key()
    if not key: return [] if task == "filter" else ""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}"}
    payload = {
        "model": "llama3-8b-8192", 
        "messages": [{"role": "user", "content": (AI_PROMPT + text) if task == "filter" else get_cv_prompt(text)}]
    }
    if task == "filter": payload["response_format"] = {"type": "json_object"}
    
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status == 200:
                res_text = (await resp.json())['choices'][0]['message']['content']
                return json.loads(res_text).get("matches", []) if task == "filter" else res_text.strip()
    except: pass
    return [] if task == "filter" else ""

# ==========================================
# 5. MAIN EXECUTION
# ==========================================

async def main():
    print("🚀 Bot started!", flush=True)
    creds = Credentials.from_service_account_info(json.loads(os.environ['GOOGLE_SHEET_CREDS']), scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    try: sheet = client.open(SHEET_NAME).sheet1 
    except Exception as e: return print(f"❌ Error Opening Sheet: {e}", flush=True)

    existing_links = set(sheet.col_values(5)[1:]) 
    next_row = len(sheet.col_values(1)) + 1 
    jobs_buffer, new_rows = [], []
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    async with aiohttp.ClientSession(headers=headers) as session:
        print("🔍 Searching Google, DDG & Jobicy...", flush=True)
        tasks = [search_google(session, q, p) for q in QUERIES for p in [1, 11]] + [search_ddg(q) for q in QUERIES]
        results = await asyncio.gather(*tasks)
        try: results.append(await fetch_jobicy_rss())
        except: pass

        for batch in results:
            for item in batch:
                if not item: continue
                link = item.get('link', '').split('?')[0]
                if link in existing_links or any(x in item.get('title', '').lower() for x in ["senior", "manager"]): continue
                item['clean_link'] = link
                jobs_buffer.append(item)
                existing_links.add(link)
        
        print(f"✅ Gathered {len(jobs_buffer)} new links. Running Deep Scrape...", flush=True)
        scrape_tasks = [fetch_full_text(session, j['clean_link'], j.get('snippet', '')) for j in jobs_buffer]
        full_texts = await asyncio.gather(*scrape_tasks)
        for i, text in enumerate(full_texts): jobs_buffer[i]['full_text'] = text

        for i in range(0, len(jobs_buffer), 5):
            chunk = jobs_buffer[i:i+5]
            print(f"🧠 AI analyzing batch {i//5 + 1}...", flush=True)
            txt = "\n".join([f"[{idx}] {j['title']} | {j['full_text'][:500]}" for idx, j in enumerate(chunk)])
            
            matches = await call_gemini(session, txt)
            if matches is None: matches = await call_groq(session, txt)
            
            if matches:
                for m in matches:
                    idx = m.get("index")
                    m_pct = m.get("match_percent", 0)
                    s_pct = m.get("suitability", 0)
                    
                    if isinstance(idx, int) and idx < len(chunk):
                        job = chunk[idx]
                        print(f"⭐ MATCH: {job['title']} (Match: {m_pct}%, Suit: {s_pct}%)", flush=True)
                        
                        cv_text = "N/A"
                        if isinstance(m_pct, (int, float)) and isinstance(s_pct, (int, float)) and m_pct >= 85 and s_pct >= 85:
                            print(f"✍️ Drafting Cover Letter for {job['title']}...", flush=True)
                            cv_text = await call_gemini(session, job['full_text'], task="cv")
                            if not cv_text: cv_text = await call_groq(session, job['full_text'], task="cv")

                        row = ["New", "AI Match", job['title'], "Unknown", job['clean_link'], str(datetime.now()), f"{m_pct}%", f"{s_pct}%", cv_text]
                        new_rows.append(row)
                        
                        kb = {"inline_keyboard": [[{"text": "✅ Apply", "callback_data": f"apply_{next_row}"}, {"text": "❌ Trash", "callback_data": f"trash_{next_row}"}]]}
                        msg_text = f"🤖 <b>JOB ALERT</b>\n\n💼 <b>{job['title']}</b>\n🎯 Match: {m_pct}%\n📈 Suitability: {s_pct}%\n📝 Cover Letter Auto-Drafted: {'Yes ✅' if cv_text != 'N/A' else 'No ❌'}\n\n🔗 {job['clean_link']}"
                        await session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": msg_text, "parse_mode": "HTML", "reply_markup": kb})
                        next_row += 1
            await asyncio.sleep(4)

    if new_rows: 
        sheet.append_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} jobs to Google Sheet.", flush=True)
    else: print("😴 No relevant jobs found this cycle.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
