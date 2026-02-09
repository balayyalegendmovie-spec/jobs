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

# ==========================================
# 1. CONFIGURATION
# ==========================================
# 🛡️ HYDRA SEARCH POOLS (6 Accounts)
SEARCH_POOLS = [
    {"key": "AIzaSyDqazTp1lPu19jIZ2nwSfyJ0keuftJQ3kk", "cx": "c7ee09e77d76e4b36"},
    {"key": "AIzaSyAISr7pZjnQCeI32fPSBouV7M4Kr-IAEhc", "cx": "b23ffd20d869042a3"},
    {"key": "AIzaSyAUgzK82Di989mhVwM9BHj2ih96ESWd3Cw", "cx": "90813680ab6ed428e"},
    {"key": "AIzaSyAX9zWw-dYB6ECIFk8ZLbQ5cpUbBjRBVnE", "cx": "94e6f9a0337d64245"},
    {"key": "AIzaSyAfX5sDpctkX1k5oEcTw7ISYRJZC8uyz70", "cx": "c4525b25604874368"},
    {"key": "AIzaSyAOjfOs-YnVe51bhK7qp5IZMUonhCs06nU", "cx": "a5f8b4ed6e9354ea0"}
]

# 🧠 AI KEYS
GEMINI_KEYS = ["AIzaSyCuigOxhFIWxcLw_iRFHcw64QYeqScINdM", "AIzaSyCxGQWZ2zl69EZWGWm747uXRUywd89rAAM", "AIzaSyAiSEI8k49cxAGKpixe2BTWRqU7cMjdWQg", "AIzaSyBOT61RyPOTBBiDw6XmLh27vAzj6XQhVkM", "AIzaSyC48NuYYc2wgupNmXvsgnytZq94ES6BRbk", "AIzaSyCRripIRlZJOj355lysWlrqMSn7q2Lq2WY"]
GROQ_KEYS = ["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", "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", "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", "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", "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"]

SHEET_NAME = "Jobs"
TELEGRAM_TOKEN = "8399820379:AAFoPDaN32FBXPFahoCZPhRW0z6ndf8y1BA"
CHAT_ID = "1808002973"
USER_PROFILE = "Fresher/Intern Cyber Security, VAPT, SOC Analyst. Python, Linux, Burp Suite."

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
# 2. LOGIC
# ==========================================
def get_search_cred(): return random.choice(SEARCH_POOLS)
def get_gemini_key(): return random.choice(GEMINI_KEYS)
def get_groq_key(): return random.choice(GROQ_KEYS)

def safe_parse_json(text):
    try: return json.loads(text)
    except:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try: return json.loads(match.group(0))
            except: pass
    return {}

async def fetch_jobicy_rss():
    # 🆕 INTEGRATED JOBICY API (RSS)
    print("📡 Scanning Jobicy (Remote)...")
    feed = feedparser.parse("https://jobicy.com/?feed=job_feed&job_categories=security-engineer&job_types=remote")
    jobs = []
    for entry in feed.entries:
        if any(x in entry.title.lower() for x in ["senior", "head", "lead"]): continue
        jobs.append({"title": entry.title, "link": entry.link, "snippet": entry.summary[:200], "source": "Jobicy"})
    return jobs

async def check_jobs_gemini(session, jobs_text):
    key = get_gemini_key()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={key}"
    prompt = f"User: {USER_PROFILE}\nFilter these. Return JSON indices {{'matches': [0, 2]}}. Rules: No Senior/Sales. Yes Analyst/Intern.\nJobs:\n{jobs_text}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                text = data['candidates'][0]['content']['parts'][0]['text']
                return safe_parse_json(text).get("matches", [])
    except: return None

async def check_jobs_groq(session, jobs_text):
    key = get_groq_key()
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}"}
    prompt = f"User: {USER_PROFILE}\nReturn ONLY JSON indices {{'matches': [0, 2]}} for relevant jobs.\nJobs:\n{jobs_text}"
    payload = {"model": "llama3-8b-8192", "messages": [{"role": "user", "content": prompt}], "response_format": {"type": "json_object"}}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return json.loads(data['choices'][0]['message']['content']).get("matches", [])
    except: return []

async def search_google(session, query, start_page):
    cred = get_search_cred()
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": cred['key'], "cx": cred['cx'], "q": query, "start": start_page}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("items", [])
    except: pass
    return []

# ==========================================
# 3. MAIN
# ==========================================
async def main():
    creds = Credentials.from_service_account_info(json.loads(os.environ['GOOGLE_SHEET_CREDS']), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(creds)
    sheet = client.open("Job_Search_Master").sheet1 
    existing_links = set(sheet.col_values(5)[1:]) 
    next_row = len(sheet.col_values(1)) + 1 
    
    jobs_buffer = []
    new_rows = []
    
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [search_google(session, q, p) for q in QUERIES for p in [1, 11]]
        results = await asyncio.gather(*tasks)
        
        # Merge Jobicy
        try: results[0].extend(await fetch_jobicy_rss())
        except: pass

        for batch in results:
            for item in batch:
                link = item.get('link', '').split('?')[0]
                if link in existing_links: continue
                if any(x in item.get('title', '').lower() for x in ["senior", "manager"]): continue
                item['clean_link'] = link
                jobs_buffer.append(item)
                existing_links.add(link)
        
        print(f"🔍 Found {len(jobs_buffer)} jobs. Running AI...")
        
        for i in range(0, len(jobs_buffer), 8):
            chunk = jobs_buffer[i:i+8]
            txt = "\n".join([f"[{idx}] {j['title']} | {j.get('snippet','')}" for idx, j in enumerate(chunk)])
            
            matches = await check_jobs_gemini(session, txt)
            if matches is None: matches = await check_jobs_groq(session, txt)
            
            for m in matches:
                if isinstance(m, int) and m < len(chunk):
                    job = chunk[m]
                    print(f"✅ {job['title']}")
                    row = ["New", "AI Match", job['title'], "Unknown", job['clean_link'], str(datetime.now())]
                    new_rows.append(row)
                    
                    # Telegram
                    kb = {"inline_keyboard": [[{"text": "✅ Apply", "callback_data": f"apply_{next_row}"}, {"text": "❌ Trash", "callback_data": f"trash_{next_row}"}]]}
                    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    payload = {"chat_id": CHAT_ID, "text": f"🤖 <b>JOB ALERT</b>\n{job['title']}\n{job['clean_link']}", "parse_mode": "HTML", "reply_markup": kb}
                    await session.post(url, json=payload)
                    next_row += 1

    if new_rows: sheet.append_rows(new_rows)

if __name__ == "__main__":
    asyncio.run(main())
