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
# 1. CONFIGURATION (THE HYDRA 🐉)
# ==========================================

SEARCH_POOLS = [
    {"key": "AIzaSyA2jTA_ju3HzDWFVUNXsUwN3UzvDbBBJhk", "cx": "c737077126efc4b44"},
    {"key": "AIzaSyAISr7pZjnQCeI32fPSBouV7M4Kr-IAEhc", "cx": "c737077126efc4b44"},
    {"key": "AIzaSyAX9zWw-dYB6ECIFk8ZLbQ5cpUbBjRBVnE", "cx": "c737077126efc4b44"},
    {"key": "AIzaSyBSEjHL6Vub2h46AnbLnfhD_WwaerrGtkI", "cx": "c737077126efc4b44"},
    {"key": "AIzaSyDqazTp1lPu19jIZ2nwSfyJ0keuftJQ3kk", "cx": "c737077126efc4b44"}
]

GEMINI_KEYS = [
    "AIzaSyCuigOxhFIWxcLw_iRFHcw64QYeqScINdM",
    "AIzaSyCxGQWZ2zl69EZWGWm747uXRUywd89rAAM",
    "AIzaSyAiSEI8k49cxAGKpixe2BTWRqU7cMjdWQg",
    "AIzaSyBOT61RyPOTBBiDw6XmLh27vAzj6XQhVkM",
    "AIzaSyC48NuYYc2wgupNmXvsgnytZq94ES6BRbk",
    "AIzaSyCRripIRlZJOj355lysWlrqMSn7q2Lq2WY"
]

GROQ_KEYS = [
    "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",
    "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",
    "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",
    "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",
    "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",
    "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"
]

SHEET_NAME = "Jobs"
TELEGRAM_TOKEN = "8399820379:AAFoPDaN32FBXPFahoCZPhRW0z6ndf8y1BA"
CHAT_ID = "1808002973"

# 💎 UPDATED USER PROFILE FROM RESUME
USER_PROFILE = """
Candidate: G Venkata Shashank
Role: Fresher/Intern in Cyber Security, VAPT, penetration tester
Education: B.Tech CSE 
Experience: Cyber Security Intern (Vulnerability assessments, OWASP Top 10, Burp Suite, Metasploitable)
Skills: C, Java, Burp Suite, Nmap, Metasploit, Wireshark, Aircrack-ng, Wazuh, Kali Linux, ParrotOS, IoT Security
Projects: ESP32 Wireless Pentest Lab, CVE-Details Telegram Bot, Cyber News Aggregator
Certifications: CEH v13 (in progress), CEP , ISCC
"""

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

# ==========================================
# 3. JOB FETCHING LOGIC
# ==========================================

async def fetch_jobicy_rss():
    print("📡 Scanning Jobicy (Remote)...", flush=True)
    try:
        feed = feedparser.parse("https://jobicy.com/?feed=job_feed&job_categories=security-engineer&job_types=remote")
        jobs = []
        for entry in feed.entries:
            if any(x in entry.title.lower() for x in ["senior", "head", "lead"]): continue
            jobs.append({"title": entry.title, "link": entry.link, "snippet": entry.summary[:200], "source": "Jobicy"})
        return jobs
    except Exception as e:
        print(f"⚠️ Jobicy Error: {e}", flush=True)
        return []

async def search_google(session, query, start_page):
    cred = get_search_cred()
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": cred['key'], "cx": cred['cx'], "q": query, "start": start_page}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("items", [])
            else:
                print(f"⚠️ Google Search Error {resp.status}: {await resp.text()}", flush=True)
    except Exception as e: 
        print(f"⚠️ Connection Error: {e}", flush=True)
    return []

# ==========================================
# 4. AI FILTERING LOGIC
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

async def check_jobs_gemini(session, jobs_text):
    key = get_gemini_key()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={key}"
    payload = {"contents": [{"parts": [{"text": AI_PROMPT + jobs_text}]}]}
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                text = data['candidates'][0]['content']['parts'][0]['text']
                return safe_parse_json(text).get("matches", [])
            else:
                print(f"⚠️ Gemini Error {resp.status}", flush=True)
    except Exception as e: pass
    return None

async def check_jobs_groq(session, jobs_text):
    key = get_groq_key()
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}"}
    payload = {"model": "llama3-8b-8192", "messages": [{"role": "user", "content": AI_PROMPT + jobs_text}], "response_format": {"type": "json_object"}}
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return json.loads(data['choices'][0]['message']['content']).get("matches", [])
            else:
                print(f"⚠️ Groq Error {resp.status}", flush=True)
    except Exception as e: pass
    return []

# ==========================================
# 5. MAIN EXECUTION
# ==========================================

async def main():
    print("🚀 Bot started!", flush=True)
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GOOGLE_SHEET_CREDS']),
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open("Job_Search_Master").sheet1 
    except Exception as e:
        print(f"❌ Error Opening Sheet: {e}", flush=True)
        return

    # ⚠️ Kept exactly as column 5 (E) to match your existing sheet
    existing_links = set(sheet.col_values(5)[1:]) 
    next_row = len(sheet.col_values(1)) + 1 
    
    jobs_buffer = []
    new_rows = []
    
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        print("🔍 Searching Google & Jobicy...", flush=True)
        tasks = [search_google(session, q, p) for q in QUERIES for p in [1, 11]]
        results = await asyncio.gather(*tasks)
        
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
        
        print(f"✅ Gathered {len(jobs_buffer)} total candidates. Starting AI Filter...", flush=True)
        
        for i in range(0, len(jobs_buffer), 8):
            chunk = jobs_buffer[i:i+8]
            print(f"🧠 Asking AI to analyze batch {i//8 + 1}...", flush=True)
            txt = "\n".join([f"[{idx}] {j['title']} | {j.get('snippet','')}" for idx, j in enumerate(chunk)])
            
            matches = await check_jobs_gemini(session, txt)
            if matches is None: matches = await check_jobs_groq(session, txt)
            
            if matches:
                for m in matches:
                    idx = m.get("index")
                    match_pct = m.get("match_percent", "N/A")
                    suitability = m.get("suitability", "N/A")
                    
                    if isinstance(idx, int) and idx < len(chunk):
                        job = chunk[idx]
                        print(f"⭐ MATCH FOUND: {job['title']} (Match: {match_pct}%, Suitability: {suitability}%)", flush=True)
                        
                        # ⚠️ Appending exactly 8 columns (Status, Verdict, Role, Company, Link, Date, Match%, Suitability%)
                        row = [
                            "New",                  # Col 1 (A)
                            "AI Match",             # Col 2 (B)
                            job['title'],           # Col 3 (C)
                            "Unknown",              # Col 4 (D)
                            job['clean_link'],      # Col 5 (E)
                            str(datetime.now()),    # Col 6 (F)
                            f"{match_pct}%",        # Col 7 (G)
                            f"{suitability}%"       # Col 8 (H)
                        ]
                        new_rows.append(row)
                        
                        kb = {"inline_keyboard": [[{"text": "✅ Apply", "callback_data": f"apply_{next_row}"}, {"text": "❌ Trash", "callback_data": f"trash_{next_row}"}]]}
                        msg_text = f"🤖 <b>JOB ALERT</b>\n\n💼 <b>{job['title']}</b>\n🎯 Profile Match: {match_pct}%\n📈 Fresher Suitability: {suitability}%\n\n🔗 {job['clean_link']}"
                        
                        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                        payload = {"chat_id": CHAT_ID, "text": msg_text, "parse_mode": "HTML", "reply_markup": kb}
                        await session.post(url, json=payload)
                        next_row += 1
                        
            await asyncio.sleep(3)

    if new_rows: 
        sheet.append_rows(new_rows)
        print(f"💾 Saved {len(new_rows)} jobs to Google Sheet.", flush=True)
    else:
        print("😴 No relevant jobs found this cycle.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
