
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  CELL 1 — INSTALL DEPENDENCIES                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
import time
import random
from typing import Dict
from datetime import datetime, timezone
import fitz          # PyMuPDF
import pdfplumber
import requests
import re
import time
import json
import math
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime, timezone

# 🔥 Supabase credentials
SUPABASE_URL = "https://uankwdgpnouwmtgcainy.supabase.co"
SUPABASE_KEY = "sb_publishable_8a7DY7P5uPa8zZmQF9OKSQ_JLLM_aJt"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  CELL 2 — INPUT PARAMETERS  (edit these before running)                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ── REQUIRED ──────────────────────────────────────────────────────────────────
   # path to the candidate's resume PDF
def simulated_ml_model(platform_links:dict[str, str]):
  print("🚀 ML Model started...")
  platform_links = {k.lower(): v for k, v in platform_links.items()}
  time.sleep(random.randint(30, 60))
  RESUME_PDF_PATH = ""
  # ── OPTIONAL PROFILE LINKS  (leave as "" to skip that platform) ───────────────
  MANUAL_LINKEDIN      = platform_links.get("linkedin", "")
  MANUAL_GITHUB        = platform_links.get("github", "")
  MANUAL_LEETCODE      = platform_links.get("leetcode", "")
  MANUAL_HACKERRANK    = platform_links.get("hackerrank", "")
  MANUAL_STACKOVERFLOW = platform_links.get("stack_overflow", "")

  # ── API CREDENTIALS ───────────────────────────────────────────────────────────
  # Option A: set directly here
  AZURE_OPENAI_ENDPOINT   = "https://linked.openai.azure.com/openai/v1/"
  AZURE_OPENAI_DEPLOYMENT = "gpt-5.4-mini"
  AZURE_OPENAI_API_KEY    = "CWcXBMalpGqTsxcKFF3movpCCR0xGwpQtMFEfIZEDTEd6oTsFtdlJQQJ99CDACYeBjFXJ3w3AAABACOGYHIY"   # paste your Azure OpenAI key

  GITHUB_TOKEN       = ""   # optional — raises rate limit
  DATAMAGNET_TOKEN   = "8e292d40c1dcec5d2e8ab5b6a6a13a8db142935ba936dad31375b4e781991c7d"   # required for LinkedIn
  KAGGLE_USERNAME    = ""   # required for Kaggle
  KAGGLE_KEY         = ""   # required for Kaggle
  SO_API_KEY         = ""   # optional — raises quota

  # Option B: read from environment variables (safer for shared notebooks)
  import os
  if not AZURE_OPENAI_API_KEY:  AZURE_OPENAI_API_KEY  = os.getenv("AZURE_OPENAI_API_KEY", "")
  if not GITHUB_TOKEN:           GITHUB_TOKEN           = os.getenv("GITHUB_TOKEN", "")
  if not DATAMAGNET_TOKEN:       DATAMAGNET_TOKEN       = os.getenv("DATAMAGNET_TOKEN", "")
  if not KAGGLE_USERNAME:        KAGGLE_USERNAME        = os.getenv("KAGGLE_USERNAME", "")
  if not KAGGLE_KEY:             KAGGLE_KEY             = os.getenv("KAGGLE_KEY", "")
  if not SO_API_KEY:             SO_API_KEY             = os.getenv("SO_API_KEY", "")

  print("✅ Input parameters set")
  print(f"   Resume PDF       : {RESUME_PDF_PATH}")
  print(f"   LinkedIn         : {MANUAL_LINKEDIN or '(auto-detect from PDF)'}")
  print(f"   GitHub           : {MANUAL_GITHUB or '(auto-detect from PDF)'}")
  print(f"   LeetCode         : {MANUAL_LEETCODE or '(auto-detect from PDF)'}")
  print(f"   HackerRank       : {MANUAL_HACKERRANK or '(auto-detect from PDF)'}")
  print(f"   StackOverflow    : {MANUAL_STACKOVERFLOW or '(auto-detect from PDF)'}")
  print(f"   Azure OpenAI     : {'✓ set' if AZURE_OPENAI_API_KEY else '✗ not set — LLM scoring will be skipped'}")
  print(f"   GitHub Token     : {'✓ set' if GITHUB_TOKEN else 'not set (60 req/hr limit)'}")
  print(f"   DataMagnet       : {'✓ set' if DATAMAGNET_TOKEN else 'not set — LinkedIn will be skipped'}")

  """## 📄 Stage 1 — Scraper Pipeline"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 3 — SHARED IMPORTS & UTILITIES                                        ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  
  # ── Retry GET / POST ──────────────────────────────────────────────────────────
  def _get(url, headers=None, params=None, retries=3, delay=1.5):
      for attempt in range(retries):
          try:
              r = requests.get(url, headers=headers, params=params, timeout=15)
              if r.status_code == 429:
                  wait = int(r.headers.get("Retry-After", delay * (attempt + 1) * 2))
                  print(f"  ⏳ Rate limited — waiting {wait}s")
                  time.sleep(wait)
                  continue
              return r
          except requests.exceptions.Timeout:
              print(f"  ⏳ Timeout (attempt {attempt+1}/{retries})")
              time.sleep(delay * (attempt + 1))
          except Exception as e:
              print(f"  ✗ Request error: {e}")
              break
      return None

  def _post(url, json_body, headers=None, retries=3, delay=1.5):
      for attempt in range(retries):
          try:
              r = requests.post(url, json=json_body, headers=headers, timeout=15)
              if r.status_code == 429:
                  time.sleep(delay * (attempt + 1) * 2)
                  continue
              return r
          except Exception as e:
              print(f"  ✗ Request error: {e}")
              break
      return None

  def _slug(url, fallback=None):
      if not url:
          return fallback
      parts = [p for p in url.rstrip("/").split("/") if p]
      slug = parts[-1].split("?")[0].split("#")[0] if parts else None
      return slug or fallback

  print("✅ Utilities ready")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 4 — RESUME PARSER (extract links, email, raw text)                   ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  '''import os

  if not os.path.exists(RESUME_PDF_PATH):
      raise FileNotFoundError(f"Resume PDF not found: {RESUME_PDF_PATH}\n"
                              "Please update RESUME_PDF_PATH in Cell 2.")

  # Clickable hyperlinks embedded in PDF
  doc = fitz.open(RESUME_PDF_PATH)
  clickable_links = []
  for page in doc:
      for link in page.get_links():
          uri = link.get("uri", "")
          if uri:
              clickable_links.append(uri)
  doc.close()

  # Visible text
  _raw = ""
  with pdfplumber.open(RESUME_PDF_PATH) as pdf:
      for page in pdf.pages:
          t = page.extract_text()
          if t:
              _raw += t + "\n"

  URL_RE   = r'(https?://[^\s,)<>\"]+|www\.[^\s,)<>\"]+)'
  EMAIL_RE = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

  text_links = re.findall(URL_RE, _raw)
  all_links  = list(dict.fromkeys(clickable_links + text_links))

  _emails = re.findall(EMAIL_RE, _raw)
  email   = _emails[0] if _emails else None

  clean_text = re.sub(r'\n{3,}', '\n\n',
              re.sub(EMAIL_RE, '',
              re.sub(URL_RE, '', _raw))).strip()

  resume_content = {"raw": _raw.strip(), "clean": clean_text}

  # Classify links by platform
  PLATFORM_RULES = {
      "linkedin": "linkedin", "github": "github", "gitlab": "gitlab",
      "twitter": "twitter", "x.com": "twitter", "leetcode": "leetcode",
      "kaggle": "kaggle", "medium": "blog", "dev.to": "blog",
      "stackoverflow": "stackoverflow", "hackerrank": "hackerrank",
      "codepen": "codepen", "huggingface": "huggingface",
      "vercel": "portfolio", "netlify": "portfolio",
      "behance": "portfolio", "dribbble": "portfolio",
  }

  def classify_link(url):
      try:
          domain = urlparse(url).netloc.lower().replace("www.", "")
      except Exception:
          domain = url.lower()
      for keyword, category in PLATFORM_RULES.items():
          if keyword in domain:
              return category
      return "other"

  _seg = {}
  for link in all_links:
      cat = classify_link(link)
      _seg.setdefault(cat, []).append(link)

  def _pick(manual, key):
      return manual.strip() if manual.strip() else _seg.get(key, [None])[0]
'''
  linkedin      = MANUAL_LINKEDIN
  github        = MANUAL_GITHUB
  leetcode      = MANUAL_LEETCODE
  hackerrank    = MANUAL_HACKERRANK
  stackoverflow = MANUAL_STACKOVERFLOW

  print(f"   github        = {github}")
  print(f"   leetcode      = {leetcode}")
  print(f"   hackerrank    = {hackerrank}")
  print(f"   stackoverflow = {stackoverflow}")
  print(f"   linkedin      = {linkedin}")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 5 — GITHUB SCRAPER                                                   ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  github_profile = {}
  github_repos   = []

  if github:
      try:
          gh_user = _slug(github)
          GH_HDR  = {"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
          GH_HDR["Accept"] = "application/vnd.github+json"

          res  = _get(f"https://api.github.com/users/{gh_user}", headers=GH_HDR)
          prof = res.json() if res else {}

          all_repos, url = [], f"https://api.github.com/users/{gh_user}/repos?per_page=100&sort=pushed"
          while url:
              r = _get(url, headers=GH_HDR)
              if not r: break
              data = r.json()
              if isinstance(data, list):
                  all_repos.extend(data)
              nxt = re.search(r'<([^>]+)>;\s*rel="next"', r.headers.get("Link", ""))
              url = nxt.group(1) if nxt else None
              time.sleep(0.3)

          original_repos  = [r for r in all_repos if not r.get("fork")]
          total_stars     = sum(r.get("stargazers_count", 0) for r in original_repos)
          total_forks_got = sum(r.get("forks_count", 0) for r in original_repos)
          languages_used  = list({r["language"] for r in original_repos if r.get("language")})

          github_profile = {
              "gh_username":        prof.get("login"),
              "gh_name":            prof.get("name"),
              "gh_bio":             prof.get("bio"),
              "gh_company":         prof.get("company"),
              "gh_blog":            prof.get("blog"),
              "gh_location":        prof.get("location"),
              "gh_followers":       prof.get("followers"),
              "gh_following":       prof.get("following"),
              "gh_public_repos":    prof.get("public_repos"),
              "gh_account_created": prof.get("created_at"),
              "gh_total_stars":     total_stars,
              "gh_total_forks_got": total_forks_got,
              "gh_original_repos":  len(original_repos),
              "gh_languages":       ", ".join(languages_used),
              "gh_top_repo_stars":  max((r.get("stargazers_count", 0) for r in original_repos), default=0),
          }

          for r in original_repos:
              github_repos.append({
                  "repo_name":   r.get("name"),
                  "repo_url":    r.get("html_url"),
                  "stars":       r.get("stargazers_count", 0),
                  "forks":       r.get("forks_count", 0),
                  "language":    r.get("language"),
                  "description": r.get("description"),
                  "updated_at":  r.get("pushed_at"),
                  "size_kb":     r.get("size", 0),
                  "open_issues": r.get("open_issues_count", 0),
                  "topics":      ", ".join(r.get("topics", [])),
              })

          print(f"✅ GitHub — {len(original_repos)} original repos | {total_stars} stars | languages: {', '.join(languages_used[:5])}")
      except Exception as e:
          print(f"✗ GitHub error: {e}")
  else:
      print("— GitHub: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 6 — LEETCODE SCRAPER                                                 ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  leetcode_profile = {}

  LC_HDR = {
      "Content-Type": "application/json",
      "Referer":      "https://leetcode.com",
      "User-Agent":   "Mozilla/5.0",
  }

  LC_QUERY = """
  query getUserProfile($username: String!) {
    matchedUser(username: $username) {
      profile { realName ranking reputation starRating }
      submitStats { acSubmissionNum { difficulty count } }
      badges { name icon }
      languageProblemCount { languageName problemsSolved }
      tagProblemCounts {
        advanced  { tagName problemsSolved }
        intermediate { tagName problemsSolved }
        fundamental { tagName problemsSolved }
      }
    }
    userContestRanking(username: $username) {
      rating globalRanking totalParticipants topPercentage attendedContestsCount
    }
  }
  """

  if leetcode:
      try:
          lc_user = _slug(leetcode)
          if not lc_user or lc_user == "u":
              parts = [p for p in leetcode.rstrip("/").split("/") if p and p != "u"]
              lc_user = parts[-1] if parts else None

          r = _post(
              "https://leetcode.com/graphql",
              json_body={"query": LC_QUERY, "variables": {"username": lc_user}},
              headers=LC_HDR
          )

          data    = r.json().get("data", {}) if r else {}
          user    = data.get("matchedUser") or {}
          contest = data.get("userContestRanking") or {}
          profile = user.get("profile") or {}
          stats   = user.get("submitStats", {}).get("acSubmissionNum", [])

          solved = {s["difficulty"]: s["count"] for s in stats}

          tag_counts   = user.get("tagProblemCounts", {})
          advanced_tags = [(t["tagName"], t["problemsSolved"]) for t in tag_counts.get("advanced", [])]
          advanced_tags.sort(key=lambda x: x[1], reverse=True)

          langs     = user.get("languageProblemCount", [])
          lang_str  = ", ".join(f"{l['languageName']}({l['problemsSolved']})" for l in langs[:5])
          badges    = [b["name"] for b in user.get("badges", [])]

          leetcode_profile = {
              "lc_username":          lc_user,
              "lc_ranking":           profile.get("ranking"),
              "lc_total_solved":      solved.get("All", 0),
              "lc_easy_solved":       solved.get("Easy", 0),
              "lc_medium_solved":     solved.get("Medium", 0),
              "lc_hard_solved":       solved.get("Hard", 0),
              "lc_contest_rating":    contest.get("rating"),
              "lc_contest_rank":      contest.get("globalRanking"),
              "lc_top_percentage":    contest.get("topPercentage"),
              "lc_contests_attended": contest.get("attendedContestsCount"),
              "lc_star_rating":       profile.get("starRating"),
              "lc_reputation":        profile.get("reputation"),
              "lc_badges":            ", ".join(badges),
              "lc_languages":         lang_str,
              "lc_top_topics":        ", ".join(t[0] for t in advanced_tags[:5]),
          }

          print(f"✅ LeetCode — total: {solved.get('All',0)} | E:{solved.get('Easy',0)} M:{solved.get('Medium',0)} H:{solved.get('Hard',0)} | contest rating: {contest.get('rating','—')}")
      except Exception as e:
          print(f"✗ LeetCode error: {e}")
  else:
      print("— LeetCode: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 7 — HACKERRANK SCRAPER                                               ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  hackerrank_profile = {}
  HR_HDR = {"User-Agent": "Mozilla/5.0"}

  HR_DOMAINS = [
      "Problem Solving", "Python", "Java", "C", "C++",
      "SQL", "Databases", "Linux Shell", "Regex",
      "JavaScript", "Rest API", "Go", "Ruby",
  ]

  def _hr_stars(badges, domain):
      for b in badges:
          if domain.lower() in b.get("badge_name", "").lower():
              return b.get("stars", 0)
      return 0

  if hackerrank:
      try:
          hr_user = _slug(hackerrank)
          res_prof   = _get(f"https://www.hackerrank.com/rest/hackers/{hr_user}/profile", headers=HR_HDR)
          prof_data  = res_prof.json().get("model", {}) if res_prof else {}
          res_badges = _get(f"https://www.hackerrank.com/rest/hackers/{hr_user}/badges", headers=HR_HDR)
          badges     = res_badges.json().get("models", []) if res_badges else []

          hr_skills_list = [
              f"{b['badge_name']} ({b.get('stars', 0)} stars)"
              for b in badges if b.get("stars", 0) > 0
          ]

          domain_scores = {f"hr_{d.lower().replace(' ', '_')}_stars": _hr_stars(badges, d)
                          for d in HR_DOMAINS}

          hackerrank_profile = {
              "hr_username":    hr_user,
              "hr_rank":        prof_data.get("level"),
              "hr_score":       prof_data.get("score"),
              "hr_country":     prof_data.get("country"),
              "hr_skills_raw":  ", ".join(hr_skills_list),
              "hr_total_badges":len([b for b in badges if b.get("stars", 0) > 0]),
              **domain_scores,
          }

          print(f"✅ HackerRank — {hackerrank_profile['hr_total_badges']} active badges | {', '.join(hr_skills_list[:4])}")
      except Exception as e:
          print(f"✗ HackerRank error: {e}")
  else:
      print("— HackerRank: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 8 — LINKEDIN SCRAPER (DataMagnet API)                                ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  linkedin_profile = {}
  linkedin_raw_data = {}   # kept for LLM stage

  if linkedin and DATAMAGNET_TOKEN:
      try:
          r = requests.post(
              "https://api.datamagnet.co/api/v1/linkedin/person",
              headers={"Authorization": f"Bearer {DATAMAGNET_TOKEN}", "Content-Type": "application/json"},
              json={"url": linkedin}, timeout=30
          )
          raw = r.json()
          data = raw.get("message", raw) if isinstance(raw, dict) else {}
          linkedin_raw_data = data

          experiences = data.get("experience") or data.get("experiences") or []
          exp_titles    = [e.get("job_title") or e.get("title", "") for e in experiences]
          exp_companies = [e.get("company_name") or e.get("company", "") for e in experiences]

          def _months(exp):
              try:
                  fmt = "%m-%Y"
                  start = datetime.strptime(str(exp.get("job_started_on") or exp.get("start_date", ""))[:7], fmt)
                  end_raw = exp.get("job_ended_on") or exp.get("end_date") or ""
                  end = datetime.strptime(str(end_raw)[:7], fmt) if end_raw else datetime.now()
                  return max(0, (end - start).days // 30)
              except Exception:
                  return 0

          total_exp_months = sum(_months(e) for e in experiences)
          education  = data.get("education") or []
          skills     = data.get("skills") or []
          skill_names= [s if isinstance(s, str) else s.get("name", "") for s in skills]
          certs      = data.get("certification") or data.get("certifications") or []
          recs       = data.get("recommendations_received") or data.get("recommendations") or []

          linkedin_profile = {
              "li_name":               data.get("full_name") or data.get("display_name"),
              "li_headline":           data.get("profile_headline") or data.get("headline"),
              "li_summary":            data.get("description") or data.get("summary") or data.get("about"),
              "li_location":           data.get("location"),
              "li_profile_url":        data.get("profile_link") or linkedin,
              "li_followers":          data.get("followers") or data.get("followers_count"),
              "li_connections":        data.get("connections") or data.get("connections_count"),
              "li_num_positions":      len(experiences),
              "li_total_exp_months":   total_exp_months,
              "li_exp_titles":         " | ".join(str(t) for t in exp_titles),
              "li_exp_companies":      " | ".join(str(c) for c in exp_companies),
              "li_num_education":      len(education),
              "li_edu_details":        " | ".join(
                  f"{e.get('university_name','')} ({', '.join(e.get('fields_of_study',[]) if isinstance(e.get('fields_of_study'), list) else [str(e.get('fields_of_study',''))])})" for e in education
              ),
              "li_num_skills":         len(skill_names),
              "li_skills":             ", ".join(skill_names[:30]),
              "li_num_certs":          len(certs),
              "li_cert_names":         ", ".join(c.get("name", "") if isinstance(c, dict) else str(c) for c in certs),
              "li_num_recommendations":len(recs),
              "li_has_photo":          bool(data.get("avatar_url") or data.get("profile_picture")),
              "li_has_summary":        bool(data.get("description") or data.get("summary")),
              "li_has_experience":     len(experiences) > 0,
              "li_has_education":      len(education) > 0,
              "li_has_skills":         len(skill_names) > 0,
              "li_has_certs":          len(certs) > 0,
              "li_has_recommendations":len(recs) > 0,
              "li_current_company":    data.get("current_company_name"),
              "li_country":            data.get("country"),
          }

          print(f"✅ LinkedIn — {linkedin_profile.get('li_name')} | {len(experiences)} positions | {len(skill_names)} skills | {total_exp_months} months exp")
      except Exception as e:
          print(f"✗ LinkedIn error: {e}")
  elif not DATAMAGNET_TOKEN:
      print("— LinkedIn: DATAMAGNET_TOKEN not set — skipping")
  else:
      print("— LinkedIn: no URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 9 — STACK OVERFLOW SCRAPER                                           ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  stackoverflow_profile = {}

  def _so_user_id(url_or_id):
      if not url_or_id: return None
      s = str(url_or_id).strip()
      if s.isdigit(): return s
      m = re.search(r'/users/(\d+)', s)
      return m.group(1) if m else None

  so_id = _so_user_id(stackoverflow)

  if so_id:
      try:
          SO_BASE   = "https://api.stackexchange.com/2.3"
          SO_PARAMS = {"site": "stackoverflow"}
          if SO_API_KEY: SO_PARAMS["key"] = SO_API_KEY

          r_prof = _get(f"{SO_BASE}/users/{so_id}", params=SO_PARAMS)
          items  = r_prof.json().get("items", []) if r_prof else []
          prof   = items[0] if items else {}

          r_tags = _get(f"{SO_BASE}/users/{so_id}/tags",
                        params={**SO_PARAMS, "pagesize": 20, "sort": "activity"})
          tags   = r_tags.json().get("items", []) if r_tags else []
          time.sleep(0.5)

          r_ans   = _get(f"{SO_BASE}/users/{so_id}/answers",
                        params={**SO_PARAMS, "pagesize": 30, "sort": "votes", "filter": "withbody"})
          answers = r_ans.json().get("items", []) if r_ans else []
          time.sleep(0.5)

          badge_counts  = prof.get("badge_counts", {})
          top_tags      = [t["name"] for t in tags]
          accepted_ans  = sum(1 for a in answers if a.get("is_accepted"))
          avg_ans_score = round(sum(a.get("score", 0) for a in answers) / max(len(answers), 1), 1)

          stackoverflow_profile = {
              "so_user_id":         so_id,
              "so_display_name":    prof.get("display_name"),
              "so_reputation":      prof.get("reputation", 0),
              "so_answer_count":    prof.get("answer_count", 0),
              "so_question_count":  prof.get("question_count", 0),
              "so_gold_badges":     badge_counts.get("gold", 0),
              "so_silver_badges":   badge_counts.get("silver", 0),
              "so_bronze_badges":   badge_counts.get("bronze", 0),
              "so_accepted_answers":accepted_ans,
              "so_avg_answer_score":avg_ans_score,
              "so_top_tags":        ", ".join(top_tags[:10]),
              "so_account_created": prof.get("creation_date"),
              "so_last_access":     prof.get("last_access_date"),
              "so_profile_url":     prof.get("link"),
          }

          print(f"✅ Stack Overflow — rep: {prof.get('reputation',0):,} | answers: {prof.get('answer_count',0)} | top tags: {', '.join(top_tags[:5])}")
      except Exception as e:
          print(f"✗ Stack Overflow error: {e}")
  else:
      print("— Stack Overflow: no user ID/URL found, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 10 — BUILD MASTER PROFILE & SAVE INTERMEDIATE CSVs                  ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import math as _math

  def _is_numeric(val):
      if val is None: return False
      if isinstance(val, bool): return True
      if isinstance(val, (int, float)):
          return not (isinstance(val, float) and _math.isnan(val))
      try:
          float(str(val).strip()); return True
      except (ValueError, TypeError):
          return False

  def split_platform(profile_dict):
      numeric, contextual = {}, {}
      for k, v in profile_dict.items():
          if _is_numeric(v): numeric[k] = v
          else: contextual[k] = v
      return numeric, contextual

  def save_platform_csvs(platform, profile_dict, extra_numeric=None, extra_contextual=None):
      if not profile_dict:
          return {platform: {'numeric': 0, 'contextual': 0, 'status': 'skipped'}}
      num_d, ctx_d = split_platform(profile_dict)
      if extra_numeric:
          for k, v in extra_numeric.items():
              num_d[k] = v; ctx_d.pop(k, None)
      if extra_contextual:
          for k, v in extra_contextual.items():
              ctx_d[k] = v; num_d.pop(k, None)
      pd.DataFrame([num_d]).to_csv(f'{platform}_numeric.csv', index=False)
      pd.DataFrame([ctx_d]).to_csv(f'{platform}_contextual.csv', index=False)
      return {platform: {'numeric': len(num_d), 'contextual': len(ctx_d), 'status': 'saved'}}

  # ── Save per-platform CSVs ─────────────────────────────────────────────────────
  save_platform_csvs('linkedin', linkedin_profile,
      extra_numeric={k: v for k, v in linkedin_profile.items() if k in {
          'li_followers','li_connections','li_num_positions','li_total_exp_months',
          'li_num_education','li_num_skills','li_num_certs','li_num_recommendations',
          'li_has_photo','li_has_summary','li_has_experience','li_has_education',
          'li_has_skills','li_has_certs','li_has_recommendations'}},
      extra_contextual={k: v for k, v in linkedin_profile.items() if k in {
          'li_name','li_headline','li_summary','li_location','li_profile_url',
          'li_exp_titles','li_exp_companies','li_edu_details','li_skills',
          'li_cert_names','li_current_company','li_country'}})

  save_platform_csvs('github', github_profile,
      extra_numeric={k: v for k, v in github_profile.items() if k in {
          'gh_followers','gh_following','gh_public_repos','gh_total_stars',
          'gh_total_forks_got','gh_original_repos','gh_top_repo_stars'}},
      extra_contextual={k: v for k, v in github_profile.items() if k in {
          'gh_bio','gh_company','gh_blog','gh_location','gh_username',
          'gh_name','gh_languages','gh_account_created'}})

  save_platform_csvs('leetcode', leetcode_profile,
      extra_numeric={k: v for k, v in leetcode_profile.items() if k in {
          'lc_total_solved','lc_easy_solved','lc_medium_solved','lc_hard_solved',
          'lc_contest_rating','lc_contest_rank','lc_top_percentage',
          'lc_contests_attended','lc_star_rating','lc_reputation','lc_ranking'}},
      extra_contextual={k: v for k, v in leetcode_profile.items() if k in {
          'lc_username','lc_badges','lc_languages','lc_top_topics'}})

  save_platform_csvs('hackerrank', hackerrank_profile,
      extra_numeric={k: v for k, v in hackerrank_profile.items()
                    if k.endswith('_stars') or k in {'hr_rank','hr_score','hr_total_badges'}},
      extra_contextual={k: v for k, v in hackerrank_profile.items() if k in {
          'hr_username','hr_skills_raw','hr_country'}})

  save_platform_csvs('stackoverflow', stackoverflow_profile,
      extra_numeric={k: v for k, v in stackoverflow_profile.items() if k in {
          'so_reputation','so_answer_count','so_question_count',
          'so_gold_badges','so_silver_badges','so_bronze_badges',
          'so_accepted_answers','so_avg_answer_score',
          'so_account_created','so_last_access'}},
      extra_contextual={k: v for k, v in stackoverflow_profile.items() if k in {
          'so_user_id','so_display_name','so_top_tags','so_profile_url'}})

  # Resume contextual
  pd.DataFrame([{'email': "none", 'resume_text': "none",
                'resume_raw': "none"}]).to_csv('resume_contextual.csv', index=False)

  # Master profile CSV
  all_data = {
      'email': "email", 'linkedin_url': linkedin, 'github_url': github,
      'leetcode_url': leetcode, 'hackerrank_url': hackerrank,
      'stackoverflow_url': stackoverflow, 'kaggle_url': "kaggle",
      'resume_text': "resume_content['clean']",
      **github_profile, **leetcode_profile, **hackerrank_profile,
      **linkedin_profile, **stackoverflow_profile,
  }
  pd.DataFrame([all_data]).to_csv('candidate_profile.csv', index=False)

  # GitHub repos
  df_github_repos = pd.DataFrame(github_repos) if github_repos else pd.DataFrame()
  df_github_repos.to_csv('github_repos.csv', index=False)

  # LinkedIn sub-tables
  if linkedin_raw_data:
      pd.json_normalize(linkedin_raw_data).to_csv('linkedin_profile_full.csv', index=False)
      exp_list = linkedin_raw_data.get('experience') or []
      edu_list = linkedin_raw_data.get('education') or []
      skill_list = linkedin_raw_data.get('skills') or []
      if exp_list:   pd.DataFrame(exp_list).to_csv('linkedin_experience.csv', index=False)
      if edu_list:   pd.DataFrame(edu_list).to_csv('linkedin_education.csv', index=False)
      if skill_list:
          pd.DataFrame({'skill_name': [s if isinstance(s, str) else s.get('name','') for s in skill_list]}).to_csv('linkedin_skills.csv', index=False)

  print(f"✅ Stage 1 complete — {len(all_data)} columns in master profile")
  print(f"   Platforms scraped: GitHub={bool(github_profile)}, LeetCode={bool(leetcode_profile)}, HackerRank={bool(hackerrank_profile)}, LinkedIn={bool(linkedin_profile)}, StackOverflow={bool(stackoverflow_profile)}")

  """## 🤖 Stage 2 — Sub-Metrics Scoring (LLM-assisted per-platform scores)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 11 — AZURE OPENAI CLIENT SETUP                                       ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  from openai import OpenAI

  NOW = datetime.now(timezone.utc)

  def safe(val, default=0):
      return default if (val is None or (isinstance(val, float) and math.isnan(val))) else val

  def clamp(val, lo=0.0, hi=100.0):
      return max(lo, min(hi, float(val)))

  def stars_to_score(stars, max_stars=5):
      return clamp((stars / max_stars) * 100)

  def iso_to_years_ago(iso):
      try:
          dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
          return (NOW - dt.replace(tzinfo=timezone.utc)).days / 365.25
      except Exception:
          return 0.0

  llm_client = None
  if AZURE_OPENAI_API_KEY:
      llm_client = OpenAI(base_url=AZURE_OPENAI_ENDPOINT, api_key=AZURE_OPENAI_API_KEY)
      print("✅ Azure OpenAI client ready")
  else:
      print("⚠️  No Azure OpenAI key — LLM scoring stages will use rule-based fallback scores")

  def llm_score(system_prompt, user_content):
      if not llm_client:
          return {}
      try:
          response = llm_client.chat.completions.create(
              model=AZURE_OPENAI_DEPLOYMENT,
              temperature=0.1,
              response_format={"type": "json_object"},
              messages=[
                  {"role": "system", "content": system_prompt},
                  {"role": "user",   "content": user_content},
              ],
          )
          return json.loads(response.choices[0].message.content)
      except Exception as e:
          print(f"  LLM call error: {e}")
          return {}

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 12 — GITHUB SUB-METRIC SCORER                                        ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  github_scores = {}

  def score_github(profile, repos):
      p = profile
      if not p:
          return {}

      n_repos  = safe(p.get('gh_original_repos'), 0)
      stars    = safe(p.get('gh_total_stars'), 0)
      forks    = safe(p.get('gh_total_forks_got'), 0)
      followers= safe(p.get('gh_followers'), 0)
      langs    = str(p.get('gh_languages', '')).split(', ') if p.get('gh_languages') else []

      account_age_years = iso_to_years_ago(p.get('gh_account_created', ''))

      # Rule-based scores
      repository_originality           = clamp(min(n_repos / 30, 1) * 100)
      stars_received_score             = clamp(min(stars / 50, 1) * 100)
      forks_received_score             = clamp(min(forks / 20, 1) * 100)
      language_diversity               = clamp(min(len(langs) / 8, 1) * 100)
      collaboration_network            = clamp(min(followers / 100, 1) * 100)
      repository_count_score           = clamp(min(n_repos / 20, 1) * 100)
      project_longevity                = clamp(min(account_age_years / 5, 1) * 100)

      # LLM-scored (needs contextual interpretation)
      llm_prompt = """You are a GitHub profile evaluator. Given a candidate's GitHub profile and repo list,
  score ONLY these sub-metrics (0-100 scale). Return ONLY a JSON object with these exact keys. Set -1 if data is missing.
  Keys: commit_frequency_score, contribution_graph_density_score, documentation_quality, ci_cd_usage"""

      user_content = json.dumps({
          'profile': {k: v for k, v in p.items() if k not in ('gh_bio',)},
          'top_repos': (repos[:10] if repos else [])
      }, default=str)

      llm_result = llm_score(llm_prompt, user_content)

      return {
          'repository_originality':            repository_originality,
          'stars_received_score':              stars_received_score,
          'forks_received_score':              forks_received_score,
          'language_diversity':                language_diversity,
          'collaboration_network':             collaboration_network,
          'repository_count_score':            repository_count_score,
          'project_longevity':                 project_longevity,
          'account_age_years':                 round(account_age_years, 2),
          'commit_frequency_score':            clamp(llm_result.get('commit_frequency_score', 30)),
          'contribution_graph_density_score':  clamp(llm_result.get('contribution_graph_density_score', 30)),
          'documentation_quality':             clamp(llm_result.get('documentation_quality', 30)),
          'ci_cd_usage':                       clamp(llm_result.get('ci_cd_usage', 10)),
      }

  if github_profile:
      github_scores = score_github(github_profile, github_repos)
      pd.DataFrame([github_scores]).to_csv('github_scores.csv', index=False)
      print(f"✅ GitHub sub-metrics scored → github_scores.csv")
  else:
      print("— GitHub: no profile data, skipping sub-metric scoring")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 13 — LEETCODE SUB-METRIC SCORER                                      ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  leetcode_scores = {}

  def score_leetcode(profile):
      p = profile
      if not p: return {}

      total   = safe(p.get('lc_total_solved'), 0)
      easy    = safe(p.get('lc_easy_solved'), 0)
      medium  = safe(p.get('lc_medium_solved'), 0)
      hard    = safe(p.get('lc_hard_solved'), 0)
      ranking = safe(p.get('lc_ranking'), 0)
      cr      = safe(p.get('lc_contest_rating'), 0)
      top_pct = safe(p.get('lc_top_percentage'), 100)
      attended= safe(p.get('lc_contests_attended'), 0)

      problems_solved_score    = clamp(min(total / 500, 1) * 100)
      global_ranking_score     = clamp((1 - min(ranking / 500000, 1)) * 100) if ranking else 0
      top_percentage_score     = clamp((1 - top_pct / 100) * 100) if top_pct < 100 else 0
      contest_rating_score     = clamp(min((cr - 1200) / 1300, 1) * 100) if cr > 1200 else 0
      contest_participation_score = clamp(min(attended / 20, 1) * 100)
      total_solved             = max(total, 1)
      acceptance_rate_score    = clamp(((easy + medium * 1.5 + hard * 2.5) / (total_solved * 2.5)) * 100)
      difficulty_distribution  = clamp((hard / max(total_solved, 1)) * 500)

      langs  = str(p.get('lc_languages', '')).split(', ')
      topics = str(p.get('lc_top_topics', '')).split(', ')
      language_diversity  = clamp(min(len([l for l in langs if l]) / 5, 1) * 100)
      category_coverage   = clamp(min(len([t for t in topics if t]) / 10, 1) * 100)

      return {
          'problems_solved_score':       problems_solved_score,
          'global_ranking_score':        global_ranking_score,
          'top_percentage_score':        top_percentage_score,
          'contest_rating_score':        contest_rating_score,
          'contest_participation_score': contest_participation_score,
          'acceptance_rate_score':       acceptance_rate_score,
          'difficulty_distribution':     difficulty_distribution,
          'language_diversity':          language_diversity,
          'category_coverage':           category_coverage,
      }

  if leetcode_profile:
      leetcode_scores = score_leetcode(leetcode_profile)
      pd.DataFrame([leetcode_scores]).to_csv('leetcode_scores.csv', index=False)
      print(f"✅ LeetCode sub-metrics scored → leetcode_scores.csv")
  else:
      print("— LeetCode: no profile data, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 14 — HACKERRANK SUB-METRIC SCORER                                    ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  hackerrank_scores = {}

  def score_hackerrank(profile):
      p = profile
      if not p: return {}

      total_badges = safe(p.get('hr_total_badges'), 0)
      ps_stars  = safe(p.get('hr_problem_solving_stars'), 0)
      py_stars  = safe(p.get('hr_python_stars'), 0)
      java_stars= safe(p.get('hr_java_stars'), 0)
      sql_stars = safe(p.get('hr_sql_stars'), 0)
      c_stars   = safe(p.get('hr_c_stars'), 0)
      cpp_stars = safe(p.get('hr_c++_stars') or p.get('hr_c___stars'), 0)

      all_domain_keys = [k for k in p if k.endswith('_stars')]
      all_star_vals   = [safe(p[k], 0) for k in all_domain_keys]
      avg_stars = sum(all_star_vals) / max(len([v for v in all_star_vals if v > 0]), 1)

      skill_certificates_score = clamp(stars_to_score(ps_stars))
      avg_stars_score          = clamp(stars_to_score(avg_stars))
      badges_count_score       = clamp(min(total_badges / 10, 1) * 100)
      domain_score_quality     = clamp(max(stars_to_score(ps_stars),
                                          stars_to_score(py_stars),
                                          stars_to_score(java_stars),
                                          stars_to_score(sql_stars)))

      hr_rank  = safe(p.get('hr_rank'), None)
      hr_score = safe(p.get('hr_score'), None)
      rank_score = clamp(min(hr_rank / 1000, 1) * 100) if hr_rank else 30

      return {
          'skill_certificates_score': skill_certificates_score,
          'avg_stars_score':          avg_stars_score,
          'badges_count_score':       badges_count_score,
          'domain_score_quality':     domain_score_quality,
          'rank_score':               rank_score,
      }

  if hackerrank_profile:
      hackerrank_scores = score_hackerrank(hackerrank_profile)
      pd.DataFrame([hackerrank_scores]).to_csv('hackerrank_scores.csv', index=False)
      print(f"✅ HackerRank sub-metrics scored → hackerrank_scores.csv")
  else:
      print("— HackerRank: no profile data, skipping")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 15 — LINKEDIN & STACKOVERFLOW SUB-METRIC SCORER (LLM)                ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  linkedin_scores     = {}
  stackoverflow_scores= {}

  # ── LinkedIn ──────────────────────────────────────────────────────────────────
  LINKEDIN_LLM_PROMPT = """You are an expert HR analyst. Given a LinkedIn profile (JSON), score ONLY the
  following sub-metrics on a 0-100 scale (100 = best possible). Return ONLY a JSON object with these
  exact keys. If data is missing set the value to -1.
  Keys to score:
  - employment_consistency_score (job gaps, tenure, logical career progression)
  - career_progression_trajectory (promotions, seniority growth)
  - company_prestige_score (tier of employers: FAANG=100, startup=30)
  - skill_endorsement_credibility (breadth, relevance of skills)
  - recommendation_authenticity (count + perceived quality)
  - profile_completeness (all sections filled)
  - network_size_quality (connections + follower count)
  - education_verification_score (degree relevance, institution prestige)
  - activity_frequency_score (posts, publications, featured items)
  - content_quality_score (quality of shared content)"""

  if linkedin_profile:
      li_llm = llm_score(LINKEDIN_LLM_PROMPT, json.dumps({
          'profile': linkedin_profile,
          'raw_experience': linkedin_raw_data.get('experience', [])[:5],
          'raw_education':  linkedin_raw_data.get('education', [])[:5],
      }, default=str))

      linkedin_scores = {
          'employment_consistency_score':  clamp(li_llm.get('employment_consistency_score', 40)),
          'career_progression_trajectory': clamp(li_llm.get('career_progression_trajectory', 40)),
          'company_prestige_score':        clamp(li_llm.get('company_prestige_score', 30)),
          'skill_endorsement_credibility': clamp(li_llm.get('skill_endorsement_credibility', 40)),
          'recommendation_authenticity':   clamp(li_llm.get('recommendation_authenticity', 20)),
          'profile_completeness':          clamp(li_llm.get('profile_completeness', 50)),
          'network_size_quality':          clamp(li_llm.get('network_size_quality',
                                                min(safe(linkedin_profile.get('li_connections', 0), 0) / 10, 100))),
          'education_verification_score':  clamp(li_llm.get('education_verification_score', 40)),
          'activity_frequency_score':      clamp(li_llm.get('activity_frequency_score', 20)),
          'content_quality_score':         clamp(li_llm.get('content_quality_score', 20)),
      }
      pd.DataFrame([linkedin_scores]).to_csv('linkedin_scores.csv', index=False)
      print("✅ LinkedIn sub-metrics scored → linkedin_scores.csv")
  else:
      print("— LinkedIn: no profile data, skipping")

  # ── Stack Overflow ─────────────────────────────────────────────────────────────
  def score_stackoverflow(profile):
      p = profile
      if not p: return {}
      rep       = safe(p.get('so_reputation'), 0)
      answers   = safe(p.get('so_answer_count'), 0)
      accepted  = safe(p.get('so_accepted_answers'), 0)
      gold      = safe(p.get('so_gold_badges'), 0)
      silver    = safe(p.get('so_silver_badges'), 0)
      avg_score = safe(p.get('so_avg_answer_score'), 0)

      reputation_score       = clamp(min(rep / 10000, 1) * 100)
      answer_volume_score    = clamp(min(answers / 100, 1) * 100)
      acceptance_rate_score  = clamp((accepted / max(answers, 1)) * 100)
      badge_quality_score    = clamp(min((gold * 20 + silver * 5) / 200, 1) * 100)
      answer_quality_score   = clamp(min(avg_score / 10, 1) * 100)
      tags = str(p.get('so_top_tags', '')).split(', ')
      expertise_breadth      = clamp(min(len([t for t in tags if t]) / 10, 1) * 100)

      return {
          'reputation_score':      reputation_score,
          'answer_volume_score':   answer_volume_score,
          'acceptance_rate_score': acceptance_rate_score,
          'badge_quality_score':   badge_quality_score,
          'answer_quality_score':  answer_quality_score,
          'expertise_breadth':     expertise_breadth,
      }

  if stackoverflow_profile:
      stackoverflow_scores = score_stackoverflow(stackoverflow_profile)
      pd.DataFrame([stackoverflow_scores]).to_csv('stackoverflow_scores.csv', index=False)
      print("✅ Stack Overflow sub-metrics scored → stackoverflow_scores.csv")
  else:
      print("— Stack Overflow: no profile data, skipping")

  """## 📊 Stage 3 — Mathematical Scoring (Weighted Platform Scores)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 16 — PLATFORM WEIGHTS & SCORING ENGINE                               ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import numpy as np
  import warnings
  warnings.filterwarnings('ignore')

  # ── Per-platform sub-metric weights ───────────────────────────────────────────
  LINKEDIN_WEIGHTS = {
      'employment_consistency_score':   0.20,
      'career_progression_trajectory':  0.18,
      'company_prestige_score':         0.12,
      'skill_endorsement_credibility':  0.10,
      'recommendation_authenticity':    0.10,
      'profile_completeness':           0.08,
      'network_size_quality':           0.08,
      'education_verification_score':   0.07,
      'activity_frequency_score':       0.04,
      'content_quality_score':          0.03,
  }

  GITHUB_WEIGHTS = {
      'repository_originality':           0.22,
      'commit_frequency_score':           0.20,
      'contribution_graph_density_score': 0.15,
      'project_longevity':                0.12,
      'stars_received_score':             0.07,
      'forks_received_score':             0.03,
      'documentation_quality':            0.06,
      'ci_cd_usage':                      0.02,
      'language_diversity':               0.06,
      'collaboration_network':            0.05,
      'repository_count_score':           0.02,
  }

  LEETCODE_WEIGHTS = {
      'problems_solved_score':       0.25,
      'global_ranking_score':        0.20,
      'top_percentage_score':        0.15,
      'contest_rating_score':        0.12,
      'contest_participation_score': 0.08,
      'acceptance_rate_score':       0.10,
      'difficulty_distribution':     0.05,
      'language_diversity':          0.03,
      'category_coverage':           0.02,
  }

  HACKERRANK_WEIGHTS = {
      'skill_certificates_score': 0.30,
      'avg_stars_score':          0.25,
      'domain_score_quality':     0.25,
      'badges_count_score':       0.12,
      'rank_score':               0.08,
  }

  STACKOVERFLOW_WEIGHTS = {
      'reputation_score':      0.30,
      'answer_volume_score':   0.20,
      'acceptance_rate_score': 0.20,
      'badge_quality_score':   0.15,
      'answer_quality_score':  0.10,
      'expertise_breadth':     0.05,
  }

  OVERALL_PLATFORM_WEIGHTS = {
      'LinkedIn':      0.30,
      'GitHub':        0.25,
      'LeetCode':      0.20,
      'HackerRank':    0.15,
      'StackOverflow': 0.10,
  }

  def _clamp_score(value):
      if pd.isna(value) or value < 0: return 0.0
      return float(np.clip(value, 0, 100))

  def weighted_platform_score(scores_dict, weights):
      present = {col: w for col, w in weights.items() if col in scores_dict}
      missing = [col for col in weights if col not in scores_dict]
      if not present:
          return {'platform_score': 0.0, 'breakdown': {}, 'warnings': missing}

      total_weight = sum(present.values())
      breakdown, weighted_sum = {}, 0.0
      for col, raw_w in present.items():
          norm_w       = raw_w / total_weight
          sub_score    = _clamp_score(scores_dict[col])
          contribution = sub_score * norm_w
          weighted_sum += contribution
          breakdown[col] = {
              'raw_score': round(sub_score, 2),
              'weight': round(norm_w * 100, 2),
              'contribution': round(contribution, 2),
          }
      return {'platform_score': round(weighted_sum, 2), 'breakdown': breakdown, 'warnings': missing}

  def score_to_grade(s):
      if s >= 850: return 'A+', '🟢 Excellent'
      if s >= 750: return 'A',  '🟢 Very Good'
      if s >= 650: return 'B+', '🟡 Good'
      if s >= 550: return 'B',  '🟡 Above Average'
      if s >= 450: return 'C+', '🟠 Average'
      if s >= 350: return 'C',  '🟠 Below Average'
      return 'D', '🔴 Needs Improvement'

  print("✅ Scoring engine loaded")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 17 — COMPUTE PLATFORM SCORES & OVERALL SCORE                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  PLATFORM_SCORES_MAP = {
      'LinkedIn':      (linkedin_scores,     LINKEDIN_WEIGHTS),
      'GitHub':        (github_scores,       GITHUB_WEIGHTS),
      'LeetCode':      (leetcode_scores,     LEETCODE_WEIGHTS),
      'HackerRank':    (hackerrank_scores,   HACKERRANK_WEIGHTS),
      'StackOverflow': (stackoverflow_scores,STACKOVERFLOW_WEIGHTS),
  }

  platform_results = {}
  platform_scores  = {}

  for platform, (scores_dict, weights) in PLATFORM_SCORES_MAP.items():
      if scores_dict:
          result = weighted_platform_score(scores_dict, weights)
          platform_results[platform] = result
          platform_scores[platform]  = result['platform_score']
      else:
          platform_results[platform] = None
          platform_scores[platform]  = None

  # ── Overall score with dynamic weight redistribution ─────────────────────────
  present_platforms = {p: s for p, s in platform_scores.items() if s is not None}
  absent_platforms  = {p: s for p, s in platform_scores.items() if s is None}

  present_weight_sum = sum(OVERALL_PLATFORM_WEIGHTS[p] for p in present_platforms)
  adjusted_weights = {
      p: (OVERALL_PLATFORM_WEIGHTS[p] / present_weight_sum if p in present_platforms else 0.0)
      for p in OVERALL_PLATFORM_WEIGHTS
  }

  overall_score = round(sum(present_platforms[p] * adjusted_weights[p] * 10 for p in present_platforms), 2)
  grade, lbl    = score_to_grade(overall_score)

  print('\n' + '═'*62)
  print(f'  🏅  OVERALL PLATFORM SCORE  :  {overall_score}/1000')
  print(f'  📋  Grade                   :  {grade}  {lbl}')
  if absent_platforms:
      print(f'  ⚠️   Missing platforms       :  {", ".join(absent_platforms.keys())}')
  print('═'*62)
  print()
  print(f"  {'Platform':<15} {'Score':>6}  {'Adj Wt':>8}  Status")
  print('  ' + '─'*45)
  for p in OVERALL_PLATFORM_WEIGHTS:
      s = platform_scores[p]
      adj_w = adjusted_weights[p]
      if s is not None:
          print(f"  {p:<15} {s*10:>6.1f}  {adj_w*100:>7.1f}%  ✅")
      else:
          print(f"  {p:<15} {'N/A':>6}  {'0.0%':>8}  ❌ missing")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 18 — SAVE SCORE REPORT CSV                                           ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  summary_rows = []
  for platform in OVERALL_PLATFORM_WEIGHTS:
      base_w = OVERALL_PLATFORM_WEIGHTS[platform]
      adj_w  = adjusted_weights[platform]
      ps     = platform_scores[platform]
      g, l   = score_to_grade(ps * 10) if ps is not None else ('N/A', '❌ Missing')
      summary_rows.append({
          'Platform':         platform,
          'Status':           'Present' if ps is not None else 'Missing',
          'Platform Score':   round(ps * 10, 2) if ps is not None else 'N/A',
          'Grade':            g,
          'Verdict':          l,
          'Base Weight':      f'{base_w*100:.0f}%',
          'Adjusted Weight':  f'{adj_w*100:.1f}%',
          'Weighted Contrib': round(ps * adj_w * 10, 2) if ps is not None else 0.0,
      })

  summary_df = pd.DataFrame(summary_rows)
  overall_row = pd.DataFrame([{
      'Platform': '🏅 OVERALL', 'Status': f"{len(present_platforms)}/{len(OVERALL_PLATFORM_WEIGHTS)} platforms",
      'Platform Score': overall_score, 'Grade': grade, 'Verdict': lbl,
      'Base Weight': '100%', 'Adjusted Weight': '100%', 'Weighted Contrib': overall_score,
  }])
  final_df = pd.concat([summary_df, overall_row], ignore_index=True)
  final_df.to_csv('overall_score_report.csv', index=False)
  print(final_df.to_string(index=False))
  print("\n✅ Stage 3 complete → overall_score_report.csv")

  """## 🧠 Stage 4 — LLM Scoring Pipeline (Holistic Analysis)"""

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 19 — PREPARE DATA PACKAGES FOR LLM ANALYSIS                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import textwrap

  def safe_float(val, default=0.0):
      try:
          v = float(val)
          return 0.0 if (v != v) else v
      except (TypeError, ValueError):
          return default

  candidate = all_data   # master profile dict built in Cell 10

  # Package A: Skill evidence
  pkg_skill = {
      'github': {
          'public_repos':         safe_float(candidate.get('gh_public_repos')),
          'total_stars':          safe_float(candidate.get('gh_total_stars')),
          'languages':            candidate.get('gh_languages', ''),
          'followers':            safe_float(candidate.get('gh_followers')),
          'commit_frequency':     safe_float(github_scores.get('commit_frequency_score', 0)),
          'documentation':        safe_float(github_scores.get('documentation_quality', 0)),
          'top_repos':            github_repos[:5] if github_repos else [],
      },
      'leetcode': {
          'total_solved':   safe_float(candidate.get('lc_total_solved')),
          'easy':           safe_float(candidate.get('lc_easy_solved')),
          'medium':         safe_float(candidate.get('lc_medium_solved')),
          'hard':           safe_float(candidate.get('lc_hard_solved')),
          'contest_rating': safe_float(candidate.get('lc_contest_rating')),
          'top_topics':     candidate.get('lc_top_topics', ''),
      },
      'hackerrank': {
          'total_badges':   safe_float(candidate.get('hr_total_badges')),
          'skills_raw':     candidate.get('hr_skills_raw', ''),
          'ps_stars':       safe_float(candidate.get('hr_problem_solving_stars')),
          'python_stars':   safe_float(candidate.get('hr_python_stars')),
      },
      'certifications': linkedin_profile.get('li_cert_names', ''),
  }

  # Package B: Professional identity
  pkg_identity = {
      'name':        linkedin_profile.get('li_name', ''),
      'headline':    linkedin_profile.get('li_headline', ''),
      'location':    linkedin_profile.get('li_location', ''),
      'linkedin': {
          'connections':   safe_float(linkedin_profile.get('li_connections')),
          'followers':     safe_float(linkedin_profile.get('li_followers')),
          'positions':     safe_float(linkedin_profile.get('li_num_positions')),
          'exp_months':    safe_float(linkedin_profile.get('li_total_exp_months')),
          'skills':        linkedin_profile.get('li_skills', ''),
          'exp_titles':    linkedin_profile.get('li_exp_titles', ''),
          'exp_companies': linkedin_profile.get('li_exp_companies', ''),
          'edu_details':   linkedin_profile.get('li_edu_details', ''),
      },
      'stackoverflow': {
          'reputation': safe_float(stackoverflow_profile.get('so_reputation')),
          'answers':    safe_float(stackoverflow_profile.get('so_answer_count')),
          'top_tags':   stackoverflow_profile.get('so_top_tags', ''),
      },
      'community_activities': [
          f"{e.get('role', '')} at {e.get('organization', '')}"
          for e in (linkedin_raw_data.get('volunteering') or [])
      ] or ['No volunteering data found'],
  }

  # Package C: Career behaviour
  pkg_behavior = {
      'career_history': [
          {'title': e.get('job_title', ''), 'company': e.get('company_name', ''),
          'type': e.get('employment_type', ''), 'started': e.get('job_started_on', ''),
          'ended': e.get('job_ended_on', 'Present'), 'current': e.get('job_still_working', False)}
          for e in (linkedin_raw_data.get('experience') or [])[:10]
      ],
      'education_history': [
          {'school': e.get('university_name', ''), 'degree': ', '.join(e.get('fields_of_study', []))}
          for e in (linkedin_raw_data.get('education') or [])[:5]
      ],
      'certifications_list': [
          f"{c.get('name', '')} – {c.get('authority', '')}"
          for c in (linkedin_raw_data.get('certification') or [])
      ] or ['No certifications found'],
      'platform_scores': {p: platform_scores[p] for p in platform_scores if platform_scores[p]},
      'overall_score': overall_score,
  }

  print("✅ Data packages ready for LLM analysis")

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 20 — LLM HOLISTIC ANALYSIS (generates candidate_analysis.json)       ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  import re as _re

  SYSTEM_HOLISTIC = textwrap.dedent("""
  You are an expert HR evaluation AI. Analyse the candidate data above for recuirtment purpose and return
  ONLY a valid JSON object (no markdown fences, no extra text).

  The JSON must contain exactly these two top-level keys:
    "candidate_report"  and  "recruiter_report"

  ────────────────────────────────────────────────────────────────
  CANDIDATE REPORT keys:
  ────────────────────────────────────────────────────────────────
  "overall_trust_score"  : integer 0-1000  (computed from multi-platform evidence)
  "trust_score_breakdown": {
      "technical_skills"  : int 0-250,   // coding platforms
      "experience"        : int 0-200,   // work history quality
      "education"         : int 0-150,   // degree relevance
      "community_impact"  : int 0-150,   // SO, GitHub stars/forks
      "consistency"       : int 0-150,   // cross-platform alignment
      "fraud_penalty"     : int 0-(-100) // deduct if anomalies detected
  }
  "score_label"          : string  e.g. "Promising Candidate"
  "what_this_score_means": string  2-3 sentence plain-English explanation
  "platform_breakdown"   : list of objects:
      { "platform": str, "score": float, "max": 100, "grade": str,
        "key_finding": str, "improvement_tip": str }
  "top_3_strengths"      : list of 3 strings
  "areas_to_improve"     : list of 3-5 strings
  "score_improvement_simulator": list of objects:
      { "action": str, "estimated_score_gain": int, "difficulty": "Easy|Medium|Hard",
        "time_estimate": str }
  "salary_intelligence"  : {
      "estimated_range_usd_annual": {"min": int, "max": int},
      "market_percentile"         : int,
      "justification"             : str,
      "comparable_roles"          : list of str
  }
  "verified_credential_badge": {
      "badge_level"    : "Bronze|Silver|Gold|Platinum",
      "verified_items" : list of str,
      "unverified_items": list of str,
      "badge_explanation": str
  }

  ────────────────────────────────────────────────────────────────
  RECRUITER REPORT keys:
  ────────────────────────────────────────────────────────────────
  "trust_score_summary"  : { "score": int, "label": str, "one_liner": str }
  "reliability_assessment": {
      "overall_reliability" : "Low|Medium|High|Very High",
      "tenure_consistency"  : str,
      "platform_activity_trend": str,
      "red_flags"           : list of str,
      "positive_signals"    : list of str
  }
  "skill_verification_summary": list of objects:
      { "skill": str, "verified_by": list of str, "confidence": "Low|Medium|High" }
  "fraud_risk_indicator"  : {
      "risk_level"          : "Low|Medium|High|Critical",
      "risk_score"          : int (0-100, 0=no risk),
      "anomalies_detected"  : list of str,
      "suspicious_patterns" : list of str,
      "recommendation"      : str
  }
  "platform_evidence_cards": list of objects per platform:
      { "platform": str, "account_age_days": int, "activity_level": str,
        "authenticity_signals": list of str, "concern_signals": list of str }
  "salary_intelligence"   : {
      "recommended_offer_range_usd": {"min": int, "max": int},
      "negotiation_advice"         : str,
      "market_benchmark"           : str
  }
  "interview_probe_suggestions": list of objects:
      { "category": str, "question": str, "what_to_listen_for": str,
        "follow_up": str }
    (provide at least 6 probes covering: technical depth, experience gaps,
    fraud verification, behavioral, motivation, culture fit)

  Be analytical, fair, and base everything strictly on the data provided.
  Flag any inconsistency (e.g. short tenure listed as long, no repo activity,
  very low SO reputation despite listed skills).
  """
  )

  USER_HOLISTIC = json.dumps({
      'skill_package':      pkg_skill,
      'identity_package':   pkg_identity,
      'behavior_package':   pkg_behavior,
      'mathematical_scores': {
          'overall_score':    overall_score,
          'grade':            grade,
          'platform_scores':  {p: round(s * 10, 1) if s else None for p, s in platform_scores.items()},
      },
  }, default=str)

  analysis = {}

  if llm_client:
      print("🔄 Calling LLM for holistic analysis...")
      try:
          response = llm_client.chat.completions.create(
              model=AZURE_OPENAI_DEPLOYMENT,
              temperature=0.2,
              response_format={'type': 'json_object'},
              messages=[
                  {'role': 'system', 'content': SYSTEM_HOLISTIC},
                  {'role': 'user',   'content': USER_HOLISTIC},
              ]
          )
          raw_output = response.choices[0].message.content
          analysis   = json.loads(raw_output)
          print("✅ LLM holistic analysis complete")
          print(analysis)
      except Exception as e:
          print(f"✗ LLM error: {e}")
          # Try regex extraction as fallback
          try:
              m = _re.search(r'\{.*\}', raw_output, _re.DOTALL)
              if m: analysis = json.loads(m.group(0))
          except Exception:
              pass
  else:
      print("⚠️  No LLM client — building rule-based analysis summary")
      analysis = {
          'candidate_name':         linkedin_profile.get('li_name') or 'Unknown',
          'contact_info':           'Not provided',
          'summary':                'Analysis generated without LLM. See mathematical scores for details.',
          'years_of_experience':    round(safe_float(linkedin_profile.get('li_total_exp_months', 0)) / 12, 1),
          'top_5_skills':           (linkedin_profile.get('li_skills', '') or '').split(', ')[:5],
          'platform_insights':      {p.lower(): f'Score: {round(s*10,1)}/100' if s else 'No data'
                                    for p, s in platform_scores.items()},
          'strengths':              ['Mathematical scoring complete — LLM assessment skipped'],
          'areas_for_improvement':  ['Configure AZURE_OPENAI_API_KEY for full LLM analysis'],
          'recommended_roles':      ['See detailed_assessment for role recommendations'],
          'mathematical_score':     overall_score,
          'overall_score':          overall_score,
          'score_breakdown':        {p: round(s * 10, 1) if s else 0 for p, s in platform_scores.items()},
          'hire_recommendation':    grade if grade in ('A+', 'A') else ('Yes' if grade in ('B+', 'B') else 'Neutral'),
          'detailed_assessment':    {
              'technical_depth':     f"LeetCode: {leetcode_profile.get('lc_total_solved', 0)} problems. GitHub: {github_profile.get('gh_original_repos', 0)} repos.",
              'domain_expertise':    f"HackerRank: {hackerrank_profile.get('hr_total_badges', 0)} badges. Skills: {linkedin_profile.get('li_num_skills', 0)}.",
              'competitive_ability': f"LeetCode Hard: {leetcode_profile.get('lc_hard_solved', 0)}. Contest rating: {leetcode_profile.get('lc_contest_rating', 'N/A')}.",
              'professional_brand':  f"LinkedIn connections: {linkedin_profile.get('li_connections', 'N/A')}. SO reputation: {stackoverflow_profile.get('so_reputation', 0)}.",
              'career_trajectory':   f"Experience: {round(safe_float(linkedin_profile.get('li_total_exp_months',0))/12,1)} years. Positions: {linkedin_profile.get('li_num_positions',0)}.",
          },
      }

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║  CELL 21 — ASSEMBLE & SAVE candidate_analysis.json                         ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝
  def list_to_paragraph(items):
    if not items:
        return ""
    return " ".join([f"{i+1}. {item}." for i, item in enumerate(items)])
  # Ensure mathematical score is always present
  analysis['mathematical_score'] = overall_score
  analysis['score_breakdown'] = {
      p: round(s * 10, 1) if s is not None else None
      for p, s in platform_scores.items()
  }
  analysis['grade']  = grade
  analysis['verdict']= lbl
  def safe_score(val):
    try:
        return int((val or 0) * 10)
    except:
        return 0
  # --- Add the specific format requested by user ---
  # Mapping internal keys to user-requested keys
  analysis['user_formatted_result'] = {
    "score": analysis.get("candidate_report", {}).get("overall_trust_score", 0),

    "pros": list_to_paragraph(
        analysis.get("recruiter_report", {})
        .get("reliability_assessment", {})
        .get("positive_signals", ["Good technical presence"])
    ),

    "cons": list_to_paragraph(
        analysis.get("recruiter_report", {})
        .get("reliability_assessment", {})
        .get("red_flags", ["No major risks identified"])
    ),

    "improvements": list_to_paragraph(
        analysis.get("candidate_report", {})
        .get("areas_to_improve", ["Improve platform activity"])
    ),
    "platform_scores": {
        "github": safe_score(platform_scores.get('GitHub')),
        "leetcode": safe_score(platform_scores.get('LeetCode')),
        "hackerrank": safe_score(platform_scores.get('HackerRank')),
        "linkedin": safe_score(platform_scores.get('LinkedIn')),
        "stack_overflow": safe_score(platform_scores.get('StackOverflow'))
    }}
  # Scraper metadata
  analysis['scraper_metadata'] = {
      'linkedin_url':     linkedin,
      'github_url':       github,
      'leetcode_url':     leetcode,
      'hackerrank_url':   hackerrank,
      'stackoverflow_url':stackoverflow,
      'platforms_available': list(present_platforms.keys()),
      'platforms_missing':   list(absent_platforms.keys()),
      'analysis_timestamp':  datetime.now().isoformat(),
  }

  # Raw platform profiles
  analysis['raw_profiles'] = {
      'github':        github_profile,
      'leetcode':      leetcode_profile,
      'hackerrank':    hackerrank_profile,
      'linkedin':      linkedin_profile,
      'stackoverflow': stackoverflow_profile,
  }

  # Sub-metric scores
  analysis['sub_metric_scores'] = {
      'github':        github_scores,
      'leetcode':      leetcode_scores,
      'hackerrank':    hackerrank_scores,
      'linkedin':      linkedin_scores,
      'stackoverflow': stackoverflow_scores,
  }

  # Save final output
  with open('candidate_analysis.json', 'w', encoding='utf-8') as f:
      json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)

  print("\n" + "═"*62)
  print("  ✅  PIPELINE COMPLETE")
  print("═"*62)
  print(f"  Candidate       : {analysis.get('candidate_name', 'N/A')}")
  print(f"  Overall Score   : {overall_score} / 1000  ({grade} — {lbl})")
  print(f"  User Result     : {json.dumps(analysis['user_formatted_result'], indent=2)}")
  print("═"*62)

  return analysis['user_formatted_result']

# endpoint
@app.post("/generate-score")
def generate_score(user_id: str):
    try:
        # 1️⃣ Validate user
        user = supabase.table("users").select("id").eq("id", user_id).execute()
        if not user.data:
            raise HTTPException(status_code=404, detail="User not found")

        # 2️⃣ Fetch platform accounts
        accounts_res = supabase.table("platform_accounts") \
            .select("id, platform_name, profile_url") \
            .eq("user_id", user_id) \
            .execute()

        accounts = accounts_res.data
        if not accounts:
            raise HTTPException(status_code=404, detail="No platform accounts found")

        # 3️⃣ Prepare platform links
        platform_links = {
            acc["platform_name"]: acc["profile_url"]
            for acc in accounts
        }

        print("📦 Platform Links:", platform_links)

        # 4️⃣ Run ML Model (blocking)
        result = simulated_ml_model(platform_links)

        if not result:
            raise HTTPException(status_code=404, detail="ML model failed")

        # ⏱️ Current UTC time
        current_time = datetime.now(timezone.utc).isoformat()

        # 5️⃣ Insert into candidate_score_analysis
        analysis_insert = supabase.table("candidate_score_analysis").insert({
            "user_id": user_id,
            "score": analysis['mathematical_score'],
            "pros": result["pros"],
            "cons": result["cons"],
            "improvements": result["improvements"],
            "is_fraud": False,
            "created_at": current_time
        }).execute()

        if not analysis_insert.data:
            raise HTTPException(status_code=404, detail="Failed to store analysis")

        print("💾 Stored candidate_score_analysis")

        # 6️⃣ Insert platform scores
        for acc in accounts:
            platform = acc["platform_name"]
            account_id = acc["id"]

            score = result["platform_scores"].get(platform, 0)

            insert_res = supabase.table("platform_score").insert({
                "platform_account_id": account_id,
                "score": score,
                "created_at": current_time
            }).execute()

            if not insert_res.data:
                raise HTTPException(status_code=404, detail=f"Failed to store score for {platform}")

        print("💾 Stored platform scores")

        # ✅ SUCCESS RESPONSE
        return {
            "status": "success",
            "data": result
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        print("❌ Error:", str(e))
        raise HTTPException(status_code=404, detail="Something went wrong")
'''@app.post("/test-links")
def test_links(platform_links: dict[str, str]):
    try:
        print("📦 Received:", platform_links)

        result = simulated_ml_model(platform_links)

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}'''
    
