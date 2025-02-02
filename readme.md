# Enhanced JobSpy ETL Pipeline

A robust and scalable ETL (Extract, Transform, Load) pipeline for scraping and analyzing job postings from multiple job boards including LinkedIn, Indeed, Glassdoor & ZipRecruiter.

## Enhanced Features

- **Multi-source Job Scraping**: Parallel scraping from major job boards
- **Intelligent Rate Limiting**: Per-site rate limiting with automatic throttling
- **Dual Database Storage**: 
  - SQLite for reliable persistent storage
  - DuckDB for high-performance analytics
- **Parallel Processing**: Multi-threaded job scraping with configurable worker count
- **Built-in Analytics**: Automated generation of salary and company analytics
- **Robust Error Handling**: 
  - Automatic retries with exponential backoff
  - Per-site error isolation
  - Comprehensive logging
- **Data Export**: Automated CSV exports of analytics results

## Requirements

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from runner import JobScraperETL

# Initialize the ETL pipeline
etl = JobScraperETL()

# Run the complete pipeline with 4 worker threads
etl.run_etl_pipeline(max_workers=4)
```

## Configuration

Key configuration parameters can be adjusted in the JobScraperETL class:

```python
# Rate limiting settings
self.max_requests = 45  # Maximum requests per time window
self.window_size = 60   # Time window in seconds

# Worker threads for parallel processing
max_workers = 4  # Adjust based on your system's capabilities

# Job search parameters
self.search_locations = [
    "New York, NY", "San Francisco, CA", "Seattle, WA",
    "Austin, TX", "Boston, MA", "Chicago, IL"
]

self.job_titles = [
    "software engineer", "software developer", 
    "data scientist", "machine learning engineer",
    "data engineer", "full stack developer"
]
```

## Analytics Output

The pipeline automatically generates two types of analytics:

### Salary Analytics (salary_analytics.csv)
- Average, minimum, and maximum salaries by location and job type
- Job count per location/job type combination
- Salary trends analysis

### Company Analytics (company_analytics.csv)
- Total job postings per company
- Number of unique locations per company
- Diversity of job types
- Average salary offerings

## Database Schema

The pipeline maintains synchronized SQLite and DuckDB databases with the following schema:

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
    is_remote BOOLEAN,
    job_function TEXT,
    company_industry TEXT,
    source_site TEXT,
    scrape_date TIMESTAMP,
    UNIQUE(job_url, company, title)
)
```

## Error Handling

The pipeline implements comprehensive error handling:

- **Rate Limiting**: Automatic throttling when approaching API limits
- **Retries**: Automatic retries with exponential backoff for transient failures
- **Isolation**: Errors in one scraping task don't affect others
- **Logging**: Detailed logging of all operations and errors

## Logging

Logs are written to `job_scraper.log` and include:
- Scraping progress and results
- Database operations
- Rate limiting events
- Errors and warnings

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
```
