"""Configuration settings for the job scraper ETL pipeline."""

from typing import List, Dict

# Database settings
DEFAULT_DB_PATH = "jobs.db"

# Rate limiting settings
MAX_REQUESTS_PER_WINDOW = 10
WINDOW_SIZE_SECONDS = 60

# Scraping settings
RESULTS_WANTED = 1000
MAX_JOB_AGE_HOURS = 72
COUNTRY = 'USA'

# Job search parameters
SEARCH_LOCATIONS: List[str] = [
    "New York, NY",
    "Los Angeles, CA", 
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "San Jose, CA",
    "Austin, TX",
    "Jacksonville, FL", 
    "Fort Worth, TX",
    "Columbus, OH",
    "San Francisco, CA",
    "Charlotte, NC",
    "Indianapolis, IN",
    "Seattle, WA",
    "Denver, CO",
    "Washington, DC",
    "Boston, MA",
    "Nashville, TN",
    "Oklahoma City, OK",
    "Portland, OR",
    "Las Vegas, NV",
    "Detroit, MI",
    "Memphis, TN",
    "Louisville, KY",
    "Baltimore, MD",
    "Milwaukee, WI",
    "Albuquerque, NM",
    "Tucson, AZ",
    "Atlanta, GA",
    "Miami, FL",
    "Minneapolis, MN",
    "Cleveland, OH",
    "New Orleans, LA",
    "St. Louis, MO",
    "Pittsburgh, PA",
    "Cincinnati, OH"
]

JOB_TITLES: List[str] = [
    "software engineer",
    "software developer",
    "data scientist",
    "machine learning engineer",
    "data engineer",
    "full stack developer"
]

# Job sites to scrape from
JOB_SITES: List[str] = [
    "indeed",
    "zip_recruiter",
    "glassdoor"
]

# Database schema
REQUIRED_COLUMNS: Dict[str, str] = {
    'job_id': 'job_id',
    'title': 'title',
    'company': 'company',
    'company_url': 'company_url',
    'job_url': 'job_url',
    'country': 'location_country',
    'city': 'location_city',
    'state': 'location_state',
    'description': 'description',
    'job_type': 'job_type',
    'job_function': 'job_function',
    'salary_interval': 'salary_interval',
    'salary_min_amount': 'salary_min_amount',
    'salary_max_amount': 'salary_max_amount',
    'salary_currency': 'salary_currency',
    'salary_source': 'salary_source',
    'date_posted': 'date_posted',
    'emails': 'emails',
    'is_remote': 'is_remote',
    'job_level': 'job_level',
    'company_industry': 'company_industry',
    'company_country': 'company_country',
    'company_addresses': 'company_addresses',
    'company_employees_label': 'company_employees_label',
    'company_revenue_label': 'company_revenue_label',
    'company_description': 'company_description',
    'company_logo': 'company_logo',
    'source_site': 'source_site',
    'scrape_date': 'scrape_date',
    'batch_id': 'batch_id'
}

# SQL Queries
CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT UNIQUE,
    title TEXT,
    company TEXT,
    company_url TEXT,
    job_url TEXT UNIQUE,
    location_country TEXT,
    location_city TEXT,
    location_state TEXT,
    description TEXT,
    job_type TEXT,
    job_function TEXT,
    salary_interval TEXT,
    salary_min_amount REAL,
    salary_max_amount REAL,
    salary_currency TEXT,
    salary_source TEXT,
    date_posted TIMESTAMP,
    emails TEXT,
    is_remote BOOLEAN,
    job_level TEXT,
    company_industry TEXT,
    company_country TEXT,
    company_addresses TEXT,
    company_employees_label TEXT,
    company_revenue_label TEXT,
    company_description TEXT,
    company_logo TEXT,
    source_site TEXT,
    scrape_date TIMESTAMP,
    batch_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_url, company, title)
)
"""

CREATE_BATCH_TABLE = """
CREATE TABLE IF NOT EXISTS job_batches (
    batch_id TEXT PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    total_jobs INTEGER,
    new_jobs INTEGER,
    updated_jobs INTEGER,
    status TEXT,
    error_message TEXT
)
"""

SALARY_ANALYTICS_QUERY = """
SELECT 
    location_city,
    job_type,
    COUNT(*) as job_count,
    AVG(salary_min_amount) as avg_min_salary,
    AVG(salary_max_amount) as avg_max_salary,
    MIN(salary_min_amount) as min_salary,
    MAX(salary_max_amount) as max_salary
FROM jobs
WHERE salary_min_amount IS NOT NULL
GROUP BY location_city, job_type
"""

COMPANY_ANALYTICS_QUERY = """
SELECT 
    company,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT location_city) as locations,
    COUNT(DISTINCT job_type) as job_types,
    AVG(CASE WHEN salary_min_amount IS NOT NULL THEN salary_min_amount END) as avg_min_salary
FROM jobs
GROUP BY company
""" 