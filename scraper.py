"""Job scraping operations for the ETL pipeline."""

import pandas as pd
import time
import logging
import random
from typing import Optional
from datetime import datetime
from jobspy import scrape_jobs
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dataclasses import dataclass
from threading import Lock
from config import MAX_REQUESTS_PER_WINDOW, WINDOW_SIZE_SECONDS, RESULTS_WANTED, MAX_JOB_AGE_HOURS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@dataclass
class RateLimiter:
    requests: int = 0
    window_start: float = time.time()
    lock: Lock = Lock()

class JobScraper:
    def __init__(self):
        """Initialize the job scraper with rate limiting."""
        self.logger = logging.getLogger(__name__)
        self.rate_limiters = {
            'indeed': RateLimiter(),
            'linkedin': RateLimiter(),
            'zip_recruiter': RateLimiter(),
            'glassdoor': RateLimiter()
        }
        self.proxies = ["localhost"]  # Add your proxy servers here
        
        # Supported job sites
        self.supported_sites = ['indeed', 'linkedin', 'zip_recruiter', 'glassdoor']
    
    def check_rate_limit(self, site: str) -> bool:
        """Check if we're within rate limits for a given site."""
        if site not in self.rate_limiters:
            self.logger.warning(f"Unsupported site: {site}")
            return False
            
        limiter = self.rate_limiters[site]
        with limiter.lock:
            current_time = time.time()
            if current_time - limiter.window_start > WINDOW_SIZE_SECONDS:
                limiter.requests = 0
                limiter.window_start = current_time
            
            if limiter.requests >= MAX_REQUESTS_PER_WINDOW:
                return False
            
            limiter.requests += 1
            return True
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception))
    )
    def scrape_jobs(self, location: str, search_term: str, site: str) -> Optional[pd.DataFrame]:
        """Scrape jobs for a specific location and search term from a specific site."""
        try:
            if site not in self.supported_sites:
                self.logger.error(f"Unsupported job site: {site}")
                return None
                
            if not self.check_rate_limit(site):
                self.logger.warning(f"Rate limit reached for {site}, waiting...")
                time.sleep(WINDOW_SIZE_SECONDS)
            
            self.logger.info(f"Scraping jobs for {search_term} in {location} from {site}")
            
            jobs = scrape_jobs(
                site_name=[site],
                search_term=search_term,
                location=location,
                results_wanted=RESULTS_WANTED,
                hours_old=MAX_JOB_AGE_HOURS,
                country_indeed='USA',
                linkedin_fetch_description=False,
                proxies=self.proxies,
                verbose=1
            )
            
            if jobs is not None and not jobs.empty:
                # Convert to pandas DataFrame if not already
                if not isinstance(jobs, pd.DataFrame):
                    jobs = pd.DataFrame(jobs)
                    
                # Add metadata columns
                jobs['scrape_date'] = datetime.now()
                jobs['source_site'] = site
                
                # Clean up salary data
                if 'salary_source' not in jobs.columns:
                    jobs['salary_source'] = site
                    
                return jobs
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from {site} for {location}: {str(e)}")
            raise 