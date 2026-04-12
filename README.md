# 🚀 TrustScore Scoring Engine

An AI-powered pipeline that aggregates candidate data from multiple platforms (LinkedIn, GitHub, LeetCode, HackerRank, StackOverflow, Resume) and generates **platform-wise scores + final candidate evaluation** using both **mathematical scoring** and **LLM-based analysis**.

---

## 📌 Overvie

This system performs:

- Multi-platform data scraping  
- Feature extraction (numeric + content-based)  
- Sub-metric scoring per platform  
- Weighted mathematical scoring  
- LLM-based deep candidate analysis  

---

## 🧠 Architecture Workflow

Platforms → Scraping → CSV Files → Feature Engineering → Scoring → LLM Analysis → Final Report

---

## ⚙️ Step 1: Data Collection & Scraping

### 🔹 Input Sources:
- LinkedIn
- GitHub
- LeetCode
- HackerRank
- StackOverflow
- Resume

### 🔹 Generated Files:

#### Candidate Info
- candidate_profile.csv

#### LinkedIn
- linkedin_numeric.csv
- linkedin_content.csv
- linkedin_profile_full.csv
- linkedin_experience.csv
- linkedin_skills.csv
- linkedin_education.csv

#### GitHub
- github_repos.csv
- github_numeric.csv
- github_content.csv

#### Coding Platforms
- leetcode_numeric.csv
- leetcode_content.csv
- hackerrank_numeric.csv
- hackerrank_content.csv

#### StackOverflow
- stackoverflow_content.csv

#### Kaggle
- kaggle_numeric.csv
- kaggle_content.csv

#### Resume
- resume_content.csv

---

## 📊 Step 2: Sub-Metrics Scoring Engine

### 🔹 Input:
- LinkedIn (profile, experience, skills, education)
- GitHub (all files)
- LeetCode (all files)
- HackerRank (all files)
- StackOverflow (all files)

### 🔹 Processing:

- Separate:
  - Numeric Data
  - Non-Numeric Data (Content)

- Use:
  - Feature Engineering
  - LLM (for content understanding)

### 🔹 Output:
- github_scores.csv
- hackerrank_scores.csv
- leetcode_scores.csv
- linkedin_scores.csv
- stackoverflow_scores.csv

---

## 📐 Step 3: Mathematical Scoring Engine

### 🔹 Input:
- All platform score CSVs

### 🔹 Processing:

- Normalize each platform score (/100)
- Apply weighted scoring

### 🔹 Output:

- Overall Score (out of 1000)
- Weighted contribution from each platform

---

## 🤖 Step 4: LLM-Based Candidate Analysis

### 🔹 Input:
- Candidate profile
- Resume content
- GitHub content
- StackOverflow content
- LinkedIn (skills, experience, education)

### 🔹 Processing:

- Deep profile analysis
- Skill validation
- Behavioral insights
- Coding ability estimation

### 🔹 Output:
- final_candidate_analysis.txt

---

## 🔍 Skill Reliability & Validation

### Logic Used:

- Declared skills (LinkedIn)
- Actual work (GitHub)
- Problem-solving (LeetCode, HackerRank)
- Community activity (StackOverflow)

### LLM Evaluates:

- Skill reliability
- Consistency
- Depth vs breadth
- Practical exposure

---

## 🧾 Final Outputs

- Platform Score CSVs  
- Overall Score  
- Candidate Ranking  
- LLM-based Analysis Report  

---

## 💡 Key Features

- Multi-platform aggregation  
- LLM-powered analysis  
- Hybrid scoring (numeric + AI)  
- Weighted evaluation system  
- Skill reliability detection  
- Scalable architecture  

---

## 🛠️ Tech Stack (Suggested)

- Python (Pandas, NumPy)
- LLM APIs
- Web Scraping (BeautifulSoup / Selenium)
- Streamlit
- PostgreSQL

## Setup

Create a virtual environment with Python 3.12+ and install the app dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The previous `requirements.txt` was a full environment export that included older TensorFlow/JAX pins incompatible with Python 3.12. The current dependency list is trimmed to the packages actually used by `main.py`.

---

## 👨‍💻 Author

Dewashish Dwivedi  
AI/ML Engineer
