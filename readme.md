
I'll help you create a comprehensive README.md file for your JobSpy ETL project. Based on the runner.py code, here's a well-structured README:

```markdown:README.md
# JobSpy ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline for scraping and analyzing job postings from multiple job boards including LinkedIn, Indeed, Glassdoor & ZipRecruiter.

## Features

- Multi-source job scraping from major job boards
- Intelligent rate limiting and proxy support
- Dual database storage (DuckDB and SQLite)
- Built-in analytics and reporting
- Batch processing for large datasets
- Robust error handling and logging

## Requirements

```python
pip install -r requirements.txt
```

Required packages:

- python-jobspy
- duckdb
- pandas
- sqlite3
- beautifulsoup4
- logging

## Quick Start

```python
from runner import JobScraperETL

# Initialize the ETL pipeline
etl = JobScraperETL()

# Run the complete pipeline
analytics_results = etl.run_etl_pipeline()
```

## Database Schema

The pipeline maintains two synchronized databases (DuckDB and SQLite) with the following schema:

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT UNIQUE,
    title TEXT,
    company TEXT,
    company_url TEXT,
    job_url TEXT,
    location_country TEXT,
    location_city TEXT,
    location_state TEXT,
    description TEXT,
    job_type TEXT,
    salary_interval TEXT,
    salary_min_amount REAL,
    salary_max_amount REAL,
    salary_currency TEXT,
    date_posted TIMESTAMP,
    is_remote INTEGER,
    job_function TEXT,
    company_industry TEXT,
    source_site TEXT,
    scrape_date TIMESTAMP,
    UNIQUE(job_url, company, title)
);
```

## Analytics Views

The pipeline includes pre-built analytical views:

### Salary Analytics

```sql
CREATE VIEW salary_analytics AS
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
GROUP BY location_city, job_type;
```

### Company Analytics

```sql
CREATE VIEW company_analytics AS
SELECT 
    company,
    COUNT(*) as total_jobs,
    COUNT(DISTINCT location_city) as locations,
    COUNT(DISTINCT job_type) as job_types,
    AVG(CASE WHEN salary_min_amount IS NOT NULL THEN salary_min_amount END) as avg_min_salary
FROM jobs
GROUP BY company;
```

## Configuration

Key configuration parameters:

```python
self.request_limit = 45  # Maximum requests per time window
self.time_window = 60    # Time window in seconds
self.batch_size = 1000   # Records per batch

# Supported locations
self.search_locations = [
    "New York, NY", "San Francisco, CA", "Seattle, WA", 
    "Austin, TX", "Boston, MA", "Chicago, IL"
]

# Job titles to search
self.job_titles = [
    "software engineer", "software developer", 
    "data scientist", "machine learning engineer",
    "data engineer", "full stack developer"
]
```

## Logging

The pipeline maintains detailed logs in `job_scraper.log`, capturing:

- Scraping progress and results
- Database operations
- Errors and warnings
- Rate limiting events

## Rate Limiting

The pipeline implements intelligent rate limiting:

- Maximum 45 requests per 60-second window
- Automatic throttling at 80% capacity
- Random delays between requests
- Proxy support for distributed scraping

## Error Handling

The pipeline includes comprehensive error handling:

- Connection error recovery
- Data validation
- Duplicate prevention
- Rate limit management
- Batch processing recovery

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

```

This README provides a comprehensive overview of your JobSpy ETL pipeline, including its features, setup instructions, database schema, and configuration options. The SQL schema section clearly shows the structure of your data storage, while the analytics views demonstrate the built-in analytical capabilities.

Would you like me to expand on any particular section or add additional information?
```
