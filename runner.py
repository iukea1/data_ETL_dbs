import duckdb
from datetime import datetime, timedelta
from jobspy import scrape_jobs
import pandas as pd
import time
import logging
from typing import List, Dict, Any
import random
import sqlite3

class JobScraperETL:
    def __init__(self, db_path: str = "jobs.duckdb", sqlite_path: str = "jobs.sqlite"):
        """Initialize the ETL pipeline with DuckDB and SQLite connections."""
        self.db_path = db_path
        self.sqlite_path = sqlite_path
        # Initialize logging and database first
        self.setup_logging()
        self.setup_database()
        
        # Add rate limiting parameters
        self.request_count = 0
        self.request_limit = 45  # Maximum requests per time window
        self.time_window = 60  # Time window in seconds
        self.last_request_time = time.time()
        self.batch_size = 1000  # Number of records to process in each batch
        
        # List of proxy servers - replace with your actual proxies
        self.proxies = [
            "localhost"  # Add your proxy servers here
        ]
        
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

    def setup_logging(self):
        """Configure logging for the ETL process."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('job_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def setup_database(self):
        """Create DuckDB and SQLite databases and tables if they don't exist."""
        # First setup DuckDB as before
        try:
            conn = duckdb.connect(self.db_path)
            
            # Create jobs table with improved data types for analytics
            conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id BIGINT PRIMARY KEY,
                job_id VARCHAR UNIQUE,
                title VARCHAR,
                company VARCHAR,
                company_url VARCHAR,
                job_url VARCHAR,
                location_country VARCHAR,
                location_city VARCHAR,
                location_state VARCHAR,
                description VARCHAR,
                job_type VARCHAR,
                salary_interval VARCHAR,
                salary_min_amount DOUBLE,
                salary_max_amount DOUBLE,
                salary_currency VARCHAR,
                date_posted TIMESTAMP,
                is_remote BOOLEAN,
                job_function VARCHAR,
                company_industry VARCHAR,
                source_site VARCHAR,
                scrape_date TIMESTAMP,
                UNIQUE(job_url, company, title)
            )
            """)
            
            # Create analytical views for common queries
            conn.execute("""
            CREATE VIEW IF NOT EXISTS salary_analytics AS
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
            """)
            
            conn.execute("""
            CREATE VIEW IF NOT EXISTS company_analytics AS
            SELECT 
                company,
                COUNT(*) as total_jobs,
                COUNT(DISTINCT location_city) as locations,
                COUNT(DISTINCT job_type) as job_types,
                AVG(CASE WHEN salary_min_amount IS NOT NULL THEN salary_min_amount END) as avg_min_salary
            FROM jobs
            GROUP BY company
            """)
            
            conn.close()
        except Exception as e:
            self.logger.error(f"Error setting up DuckDB database: {str(e)}")
            if 'conn' in locals():
                conn.close()

        # Now setup SQLite
        try:
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            cursor = sqlite_conn.cursor()
            
            # Create jobs table in SQLite
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
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
            )
            """)
            
            sqlite_conn.commit()
            sqlite_conn.close()
            
        except Exception as e:
            self.logger.error(f"Error setting up SQLite database: {str(e)}")
            if 'sqlite_conn' in locals():
                sqlite_conn.close()

    def generate_job_id(self, row: Dict[str, Any]) -> str:
        """Generate a unique job ID based on job details."""
        components = [
            str(row.get('job_url', '')),
            str(row.get('company', '')),
            str(row.get('title', '')),
            str(row.get('location_city', '')),
            str(row.get('date_posted', ''))
        ]
        return '_'.join(components)

    def check_rate_limit(self):
        """Check and handle rate limiting."""
        current_time = time.time()
        time_passed = current_time - self.last_request_time
        
        # Reset counter if time window has passed
        if time_passed > self.time_window:
            self.request_count = 0
            self.last_request_time = current_time
        
        # If approaching rate limit, sleep to avoid hitting it
        if self.request_count >= (self.request_limit * 0.8):  # 80% of limit
            sleep_time = random.uniform(5, 10)
            self.logger.info(f"Approaching rate limit, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self.request_count = 0
            self.last_request_time = time.time()
        
        self.request_count += 1

    def scrape_jobs_for_location(self, location: str, search_term: str) -> pd.DataFrame:
        """Scrape jobs for a specific location and search term."""
        try:
            self.logger.info(f"Scraping jobs for {search_term} in {location}")
            
            # Suppress BeautifulSoup warning
            import warnings
            from bs4 import GuessedAtParserWarning, MarkupResemblesLocatorWarning
            warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
            warnings.filterwarnings("ignore", category=GuessedAtParserWarning)
            
            job_boards = ["indeed", "google", "zip_recruiter", "glassdoor"]
            all_jobs = []
            
            for site in job_boards:
                try:
                    # Check rate limit before making request
                    self.check_rate_limit()
                    
                    jobs = scrape_jobs(
                        site_name=[site],
                        search_term=search_term,
                        location=location,
                        results_wanted=1000,
                        hours_old=72,
                        country_indeed='USA',
                        proxies=self.proxies,
                        verbose=0
                    )
                    
                    if isinstance(jobs, pd.DataFrame) and not jobs.empty:
                        all_jobs.append(jobs)
                        self.logger.info(f"Successfully scraped {len(jobs)} jobs from {site}")
                    else:
                        self.logger.warning(f"No jobs found on {site} for {search_term} in {location}")
                        
                except Exception as site_error:
                    self.logger.error(f"Error scraping {site}: {str(site_error)}")
                    continue
                
                # Add delay between job boards
                time.sleep(random.uniform(3, 7))
            
            # Combine results from all successful job boards
            if all_jobs:
                combined_jobs = pd.concat(all_jobs, ignore_index=True)
                combined_jobs['scrape_date'] = datetime.now()
                combined_jobs['source_site'] = combined_jobs['site']
                return combined_jobs
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Critical error in scrape_jobs_for_location: {str(e)}")
            return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean the scraped data."""
        if df.empty:
            return df
            
        # Generate unique job IDs
        df['job_id'] = df.apply(self.generate_job_id, axis=1)
        
        # Clean salary data
        df['salary_min_amount'] = pd.to_numeric(df['min_amount'], errors='coerce')
        df['salary_max_amount'] = pd.to_numeric(df['max_amount'], errors='coerce')
        df['salary_interval'] = df['interval']
        
        # Handle location data
        if 'country' not in df.columns:
            df['country'] = 'USA'  # Default to USA since we're only searching US locations
        if 'city' not in df.columns:
            df['city'] = df['location'].str.split(',').str[0] if 'location' in df.columns else None
        if 'state' not in df.columns:
            df['state'] = df['location'].str.split(',').str[-1].str.strip() if 'location' in df.columns else None
        
        # Clean and standardize columns
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
        df['scrape_date'] = pd.to_datetime(df['scrape_date'])
        
        # Ensure all required columns exist with default values
        required_columns = {
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
            'salary_interval': 'salary_interval',
            'salary_min_amount': 'salary_min_amount',
            'salary_max_amount': 'salary_max_amount',
            'currency': 'salary_currency',
            'date_posted': 'date_posted',
            'is_remote': 'is_remote',
            'job_function': 'job_function',
            'company_industry': 'company_industry',
            'source_site': 'source_site',
            'scrape_date': 'scrape_date'
        }
        
        # Add missing columns with None/NaN values
        for col in required_columns.keys():
            if col not in df.columns:
                df[col] = None
                self.logger.warning(f"Column {col} was missing and has been added with NULL values")
        
        # Rename columns according to mapping
        df = df.rename(columns=required_columns)
        
        # Select only the mapped columns in the correct order
        return df[list(required_columns.values())]

    def load_to_database(self, df: pd.DataFrame):
        """Load transformed data into both DuckDB and SQLite databases in batches."""
        if df.empty:
            self.logger.warning("No data to load into database")
            return
            
        # First load to DuckDB as before
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            
            # Process data in batches
            total_rows = len(df)
            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Check for duplicates before insertion
                conn.execute("""
                    WITH new_data AS (
                        SELECT 
                            ROW_NUMBER() OVER () + COALESCE((SELECT MAX(id) FROM jobs), 0) as id,
                            *
                        FROM batch_df
                    ),
                    unique_new_data AS (
                        SELECT n.*
                        FROM new_data n
                        LEFT JOIN jobs j ON 
                            n.job_url = j.job_url AND 
                            n.company = j.company AND 
                            n.title = j.title
                        WHERE j.id IS NULL
                    )
                    INSERT INTO jobs 
                    SELECT id, job_id, title, company, company_url, job_url, 
                           location_country, location_city, location_state, description,
                           job_type, salary_interval, salary_min_amount, salary_max_amount,
                           salary_currency, date_posted, is_remote, job_function,
                           company_industry, source_site, scrape_date
                    FROM unique_new_data
                """)
                
                conn.commit()
                self.logger.info(f"Processed batch {start_idx//self.batch_size + 1}, "
                               f"rows {start_idx} to {end_idx}")
            
            self.logger.info(f"Successfully processed all {total_rows} jobs")
            
        except Exception as e:
            self.logger.error(f"Error loading data to DuckDB: {str(e)}")
            
        finally:
            if conn:
                conn.close()

        # Now load to SQLite
        sqlite_conn = None
        try:
            sqlite_conn = sqlite3.connect(self.sqlite_path)
            
            # Process data in batches
            total_rows = len(df)
            for start_idx in range(0, total_rows, self.batch_size):
                end_idx = min(start_idx + self.batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Convert boolean is_remote to integer for SQLite
                batch_df['is_remote'] = batch_df['is_remote'].astype(int)
                
                # Insert data using pandas to_sql with "INSERT OR IGNORE" to handle duplicates
                batch_df.to_sql(
                    'jobs',
                    sqlite_conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=self.batch_size
                )
                
                sqlite_conn.commit()
                self.logger.info(f"Processed SQLite batch {start_idx//self.batch_size + 1}, "
                               f"rows {start_idx} to {end_idx}")
            
            # Log the actual number of rows inserted by checking the SQLite database
            cursor = sqlite_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM jobs")
            total_rows_sqlite = cursor.fetchone()[0]
            self.logger.info(f"Total rows in SQLite database: {total_rows_sqlite}")
            
        except Exception as e:
            self.logger.error(f"Error loading data to SQLite: {str(e)}")
            
        finally:
            if sqlite_conn:
                sqlite_conn.close()

    def run_analytics(self):
        """Run analytical queries on the collected data."""
        conn = duckdb.connect(self.db_path)
        
        # Salary trends by location
        salary_trends = conn.execute("""
            SELECT 
                location_city,
                job_type,
                AVG(salary_min_amount) as avg_min_salary,
                AVG(salary_max_amount) as avg_max_salary,
                COUNT(*) as job_count
            FROM jobs
            WHERE salary_min_amount IS NOT NULL
            GROUP BY location_city, job_type
            ORDER BY avg_min_salary DESC
        """).df()
        
        # Company hiring patterns
        company_patterns = conn.execute("""
            SELECT 
                company,
                COUNT(*) as total_jobs,
                COUNT(DISTINCT job_type) as unique_job_types,
                COUNT(DISTINCT location_city) as locations,
                AVG(salary_min_amount) as avg_min_salary
            FROM jobs
            GROUP BY company
            HAVING total_jobs > 1
            ORDER BY total_jobs DESC
        """).df()
        
        # Remote work trends
        remote_trends = conn.execute("""
            SELECT 
                job_type,
                COUNT(*) FILTER (WHERE is_remote) as remote_jobs,
                COUNT(*) as total_jobs,
                ROUND(COUNT(*) FILTER (WHERE is_remote) * 100.0 / COUNT(*), 2) as remote_percentage
            FROM jobs
            GROUP BY job_type
            ORDER BY remote_percentage DESC
        """).df()
        
        conn.close()
        
        return {
            'salary_trends': salary_trends,
            'company_patterns': company_patterns,
            'remote_trends': remote_trends
        }

    def run_etl_pipeline(self):
        """Execute the complete ETL pipeline."""
        total_jobs = 0
        
        for location in self.search_locations:
            for job_title in self.job_titles:
                try:
                    # Add random delay between searches to avoid rate limiting
                    time.sleep(random.uniform(2, 5))
                    
                    # Extract
                    raw_data = self.scrape_jobs_for_location(location, job_title)
                    
                    if not raw_data.empty:
                        # Transform
                        transformed_data = self.transform_data(raw_data)
                        
                        # Load
                        self.load_to_database(transformed_data)
                        
                        total_jobs += len(transformed_data)
                        
                except Exception as e:
                    self.logger.error(f"Pipeline error for {job_title} in {location}: {str(e)}")
                    continue
        
        self.logger.info(f"ETL pipeline completed. Total jobs processed: {total_jobs}")
        
        # Run analytics after pipeline completion
        analytics_results = self.run_analytics()
        self.logger.info("Analytics completed. Results available in analytics_results dictionary.")
        return analytics_results

if __name__ == "__main__":
    # Initialize and run the ETL pipeline
    etl = JobScraperETL()
    analytics_results = etl.run_etl_pipeline()
    
    # Print some analytics results
    print("\nSalary Trends by Location:")
    print(analytics_results['salary_trends'].head())
    
    print("\nTop Companies by Job Count:")
    print(analytics_results['company_patterns'].head())
    
    print("\nRemote Work Trends:")
    print(analytics_results['remote_trends'])