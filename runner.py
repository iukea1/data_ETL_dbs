"""Main runner for the job scraper ETL pipeline."""

import logging
import time
import random
import concurrent.futures
from typing import Tuple
import pandas as pd
from datetime import datetime

from config import (
    DEFAULT_DB_PATH,
    SEARCH_LOCATIONS,
    JOB_TITLES,
    JOB_SITES
)
from database import DatabaseHandler
from data_transformer import DataTransformer
from scraper import JobScraper

class JobScraperETL:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        """Initialize the ETL pipeline components."""
        self.setup_logging()
        self.db = DatabaseHandler(db_path)
        self.transformer = DataTransformer()
        self.scraper = JobScraper()
        self.logger = logging.getLogger(__name__)
    
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
    
    def process_location_title_pair(self, location: str, job_title: str, batch_id: str) -> Tuple[int, int]:
        """Process a single location and job title pair."""
        total_new_jobs = 0
        total_updated_jobs = 0
        
        for site in JOB_SITES:
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Add randomized delay between searches (increased range)
                    time.sleep(random.uniform(2, 5))
                    
                    # Extract
                    raw_data = self.scraper.scrape_jobs(location, job_title, site)
                    
                    if raw_data is not None and not raw_data.empty:
                        # Transform
                        transformed_data = self.transformer.transform_data(raw_data)
                        
                        if not transformed_data.empty:
                            # Deduplicate before saving
                            transformed_data = self.deduplicate_jobs(transformed_data)
                            
                            # Add batch metadata
                            transformed_data['scrape_date'] = datetime.now()
                            transformed_data['batch_id'] = batch_id
                            
                            # Save to database with batch tracking
                            new_jobs, updated_jobs = self.db.save_jobs(transformed_data, batch_id)
                            total_new_jobs += new_jobs
                            total_updated_jobs += updated_jobs
                            
                            # Success - break retry loop
                            break
                            
                except Exception as e:
                    retry_count += 1
                    error_msg = f"Pipeline error for {job_title} in {location} from {site}: {str(e)}"
                    
                    if "429" in str(e) or "rate limit" in str(e).lower():
                        # Rate limit hit - wait longer before retry
                        wait_time = retry_count * 10  # Exponential backoff
                        self.logger.warning(f"Rate limit reached for {site}, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        self.logger.error(error_msg)
                        
                    if retry_count == max_retries:
                        self.logger.error(f"Max retries ({max_retries}) reached for {site}")
                        continue
        
        return total_new_jobs, total_updated_jobs

    def run_pipeline(self, max_workers: int = 4):
        """Execute the complete ETL pipeline with parallel processing and batch tracking."""
        batch_id = None
        total_new_jobs = 0
        total_updated_jobs = 0
        
        try:
            # Start new batch
            batch_id = self.db.start_batch()
            self.logger.info(f"Started batch {batch_id}")
            
            job_pairs = [(loc, title) for loc in SEARCH_LOCATIONS for title in JOB_TITLES]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_pair = {
                    executor.submit(self.process_location_title_pair, loc, title, batch_id): (loc, title) 
                    for loc, title in job_pairs
                }
                
                for future in concurrent.futures.as_completed(future_to_pair):
                    loc, title = future_to_pair[future]
                    try:
                        new_jobs, updated_jobs = future.result()
                        total_new_jobs += new_jobs
                        total_updated_jobs += updated_jobs
                    except Exception as e:
                        self.logger.error(f"Pipeline error for {title} in {loc}: {str(e)}")
            
            # Complete batch
            self.db.end_batch(
                batch_id=batch_id,
                total_jobs=total_new_jobs + total_updated_jobs,
                new_jobs=total_new_jobs,
                updated_jobs=total_updated_jobs
            )
            
            self.logger.info(
                f"ETL pipeline completed. "
                f"New jobs: {total_new_jobs}, "
                f"Updated jobs: {total_updated_jobs}"
            )
            
            # Run analytics
            self.run_analytics()
            
            # Log batch statistics
            self._log_batch_stats()
            
        except Exception as e:
            error_msg = f"Pipeline failed: {str(e)}"
            self.logger.error(error_msg)
            if batch_id is not None:
                self.db.end_batch(
                    batch_id=batch_id,
                    total_jobs=total_new_jobs + total_updated_jobs,
                    new_jobs=total_new_jobs,
                    updated_jobs=total_updated_jobs,
                    error_message=error_msg
                )
            raise
    
    def run_analytics(self):
        """Run analytics and save results to CSV."""
        try:
            salary_analytics, company_analytics = self.db.run_analytics()
            
            # Save to CSV
            salary_analytics.to_csv('salary_analytics.csv', index=False)
            company_analytics.to_csv('company_analytics.csv', index=False)
            
            self.logger.info("Analytics completed and exported to CSV files")
            
        except Exception as e:
            self.logger.error(f"Error running analytics: {str(e)}")
    
    def _log_batch_stats(self, days: int = 7):
        """Log statistics for recent batch runs."""
        try:
            stats = self.db.get_batch_stats(days)
            if not stats.empty:
                self.logger.info("\nRecent Batch Statistics:")
                self.logger.info(f"Last {days} days summary:")
                self.logger.info(f"Total batches: {len(stats)}")
                self.logger.info(f"Successful batches: {len(stats[stats['status'] == 'COMPLETED'])}")
                self.logger.info(f"Failed batches: {len(stats[stats['status'] == 'FAILED'])}")
                self.logger.info(f"Total new jobs: {stats['new_jobs'].sum()}")
                self.logger.info(f"Total updated jobs: {stats['updated_jobs'].sum()}")
        except Exception as e:
            self.logger.error(f"Error logging batch stats: {str(e)}")
    def deduplicate_jobs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate jobs from the current batch and against existing database entries."""
        try:
            # Step 1: Remove duplicates within current batch
            df = df.drop_duplicates(subset=['job_url', 'company', 'title'])

            # Step 2: Check against existing database entries
            existing_jobs = pd.read_sql("SELECT job_url, company, title FROM jobs", conn)
            
            # Step 3: Keep only new jobs
            merged = df.merge(existing_jobs, on=['job_url', 'company', 'title'], 
                            how='left', indicator=True)
            return merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
                
        except Exception as e:
            self.logger.error(f"Error deduplicating jobs: {str(e)}")
            raise

if __name__ == "__main__":
    etl = JobScraperETL()
    etl.run_pipeline(max_workers=4)