from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GitHubRepoModel(BaseModel):
    repo_name: str = ""
    repo_url: str = ""
    stars: int = 0
    forks: int = 0
    language: str | None = None
    description: str | None = None
    updated_at: str | None = None
    size_kb: int = 0
    open_issues: int = 0
    topics: str = ""


class GitHubModel(BaseModel):
    profile_url: str = ""
    username: str = ""
    name: str | None = None
    bio: str | None = None
    company: str | None = None
    blog: str | None = None
    location: str | None = None
    followers: int = 0
    following: int = 0
    public_repos: int = 0
    original_repos: int = 0
    stars: int = 0
    forks_received: int = 0
    top_repo_stars: int = 0
    commits: int = 0
    languages: list[str] = Field(default_factory=list)
    account_created: str | None = None
    activity_level: str = "low"
    repos_data: list[GitHubRepoModel] = Field(default_factory=list)


class LeetCodeModel(BaseModel):
    profile_url: str = ""
    username: str = ""
    ranking: int | None = None
    problems_solved: int = 0
    easy_solved: int = 0
    medium_solved: int = 0
    hard_solved: int = 0
    contest_rating: float | None = None
    contest_rank: int | None = None
    top_percentage: float | None = None
    contests_attended: int = 0
    star_rating: float | None = None
    reputation: int | None = None
    badges: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)
    top_topics: list[str] = Field(default_factory=list)


class HackerRankModel(BaseModel):
    profile_url: str = ""
    username: str = ""
    rank: int | None = None
    score: float | None = None
    country: str | None = None
    skills: list[str] = Field(default_factory=list)
    total_badges: int = 0
    domain_stars: dict[str, int] = Field(default_factory=dict)


class LinkedInExperienceModel(BaseModel):
    experience_index: int = 0
    job_title: str = ""
    company_name: str = ""
    employment_type: str = ""
    start_date: str = ""
    end_date: str = ""
    start_year: int | None = None
    start_month: int | None = None
    end_year: int | None = None
    end_month: int | None = None
    duration_months: int = 0
    is_current: bool = False
    company_industry: str = ""
    company_headcount_range: str = ""
    company_id: str = ""
    company_url: str = ""
    company_website: str = ""
    job_location: str = ""
    job_location_city: str = ""
    job_location_state: str = ""
    job_location_country: str = ""
    job_description: str = ""
    raw_job_title: str = ""
    raw_company_name: str = ""


class LinkedInEducationModel(BaseModel):
    education_index: int = 0
    university_name: str = ""
    fields_of_study: str = ""
    start_date: str = ""
    end_date: str = ""
    start_year: int | None = None
    start_month: int | None = None
    end_year: int | None = None
    end_month: int | None = None
    duration_months: int = 0
    is_current: bool = False
    grade: str = ""
    description: str = ""
    social_url: str = ""
    university_id: str = ""
    logo: str = ""


class LinkedInSkillModel(BaseModel):
    skill_index: int = 0
    skill_name: str = ""
    endorsement_count: int | None = None


class LinkedInModel(BaseModel):
    profile_url: str = ""
    name: str | None = None
    headline: str | None = None
    summary: str | None = None
    location: str | None = None
    followers: int = 0
    connections: int = 0
    current_company: str | None = None
    country: str | None = None
    total_experience_months: int = 0
    certifications: list[dict[str, Any]] = Field(default_factory=list)
    recommendations: list[dict[str, Any]] = Field(default_factory=list)
    publications: list[dict[str, Any]] = Field(default_factory=list)
    projects: list[dict[str, Any]] = Field(default_factory=list)
    featured: list[dict[str, Any]] = Field(default_factory=list)
    volunteering: list[dict[str, Any]] = Field(default_factory=list)
    experience: list[LinkedInExperienceModel] = Field(default_factory=list)
    education: list[LinkedInEducationModel] = Field(default_factory=list)
    skills: list[LinkedInSkillModel] = Field(default_factory=list)
    timeline_metrics: dict[str, Any] = Field(default_factory=dict)
    normalized_profile: dict[str, Any] = Field(default_factory=dict)
    source_payload: dict[str, Any] = Field(default_factory=dict)


class StackOverflowModel(BaseModel):
    profile_url: str = ""
    user_id: str = ""
    display_name: str | None = None
    reputation: int = 0
    answer_count: int = 0
    question_count: int = 0
    gold_badges: int = 0
    silver_badges: int = 0
    bronze_badges: int = 0
    accepted_answers: int = 0
    avg_answer_score: float = 0.0
    top_tags: list[str] = Field(default_factory=list)
    account_created: int | str | None = None
    last_access: int | str | None = None
