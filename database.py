"""Database operations for the job scraper ETL pipeline."""

import sqlite3
import pandas as pd
from typing import Optional, Tuple
import logging
from datetime import datetime
import uuid
from config import CREATE_JOBS_TABLE, CREATE_BATCH_TABLE, SALARY_ANALYTICS_QUERY, COMPANY_ANALYTICS_QUERY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatabaseHandler:
    def __init__(self, db_path: str):
        """Initialize database connection and setup."""
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.setup_database()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection with proper settings."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def setup_database(self):
        """Create database and tables if they don't exist."""
        try:
            with self.get_connection() as conn:
                # Drop existing tables if they exist to ensure schema is up to date
                conn.execute("DROP TABLE IF EXISTS jobs")
                conn.execute("DROP TABLE IF EXISTS job_batches")
                
                # Create tables with complete schema
                conn.execute(CREATE_JOBS_TABLE)
                conn.execute(CREATE_BATCH_TABLE)
                conn.commit()
                self.logger.info("Database setup completed successfully")
        except Exception as e:
            self.logger.error(f"Error setting up database: {str(e)}")
            raise
    
    def start_batch(self) -> str:
        """Start a new batch process and return the batch ID."""
        batch_id = str(uuid.uuid4())
        try:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO job_batches (batch_id, start_time, status)
                    VALUES (?, ?, ?)
                    """,
                    (batch_id, datetime.now(), 'RUNNING')
                )
                conn.commit()
                self.logger.info(f"Started new batch with ID: {batch_id}")
            return batch_id
        except Exception as e:
            self.logger.error(f"Error starting batch: {str(e)}")
            raise
    
    def end_batch(self, batch_id: str, total_jobs: int, new_jobs: int, 
                 updated_jobs: int, error_message: Optional[str] = None):
        """Update batch status on completion."""
        status = 'COMPLETED' if error_message is None else 'FAILED'
        try:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    UPDATE job_batches 
                    SET end_time = ?, 
                        status = ?,
                        total_jobs = ?,
                        new_jobs = ?,
                        updated_jobs = ?,
                        error_message = ?
                    WHERE batch_id = ?
                    """,
                    (datetime.now(), status, total_jobs, new_jobs, 
                     updated_jobs, error_message, batch_id)
                )
                conn.commit()
                self.logger.info(
                    f"Ended batch {batch_id} with status {status}. "
                    f"Total: {total_jobs}, New: {new_jobs}, Updated: {updated_jobs}"
                )
        except Exception as e:
            self.logger.error(f"Error ending batch {batch_id}: {str(e)}")
            raise
    
    def get_existing_jobs(self) -> pd.DataFrame:
        """Retrieve existing jobs from database for deduplication."""
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(
                    """
                    SELECT job_url, company, title, 
                           salary_min_amount, salary_max_amount,
                           company_description, description
                    FROM jobs
                    """,
                    conn
                )
                self.logger.info(f"Retrieved {len(df)} existing jobs from database")
                return df
        except Exception as e:
            self.logger.error(f"Error retrieving existing jobs: {str(e)}")
            return pd.DataFrame()
    
    def save_jobs(self, df: pd.DataFrame, batch_id: str) -> Tuple[int, int]:
        """
        Save jobs to database and return tuple of (new_jobs, updated_jobs).
        Handles both inserts of new jobs and updates of existing ones.
        """
        if df.empty:
            self.logger.warning("No jobs to save")
            return 0, 0
            
        try:
            new_jobs = 0
            updated_jobs = 0
            
            with self.get_connection() as conn:
                for _, row in df.iterrows():
                    # Convert row to dictionary and add batch_id
                    row_dict = row.to_dict()
                    row_dict['batch_id'] = batch_id
                    
                    try:
                        # Try to insert new job
                        pd.DataFrame([row_dict]).to_sql(
                            'jobs', 
                            conn, 
                            if_exists='append', 
                            index=False
                        )
                        new_jobs += 1
                        
                    except sqlite3.IntegrityError:
                        # Job exists, update if needed
                        update_cols = [
                            'salary_min_amount', 'salary_max_amount',
                            'company_description', 'description',
                            'job_type', 'is_remote', 'company_industry',
                            'updated_at', 'batch_id'
                        ]
                        
                        set_clause = ', '.join([f"{col} = ?" for col in update_cols])
                        values = [row_dict.get(col) for col in update_cols]
                        values.extend([datetime.now(), batch_id])
                        values.extend([row_dict['job_url'], row_dict['company'], row_dict['title']])
                        
                        cursor = conn.execute(
                            f"""
                            UPDATE jobs 
                            SET {set_clause}
                            WHERE job_url = ? AND company = ? AND title = ?
                            """,
                            values
                        )
                        
                        if cursor.rowcount > 0:
                            updated_jobs += 1
                            
                conn.commit()
                
            self.logger.info(f"Saved {new_jobs} new jobs and updated {updated_jobs} existing jobs")
            return new_jobs, updated_jobs
            
        except Exception as e:
            self.logger.error(f"Error saving jobs to database: {str(e)}")
            raise
    
    def run_analytics(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Run analytics queries and return results."""
        try:
            with self.get_connection() as conn:
                salary_analytics = pd.read_sql(SALARY_ANALYTICS_QUERY, conn)
                company_analytics = pd.read_sql(COMPANY_ANALYTICS_QUERY, conn)
                
                self.logger.info(
                    f"Generated analytics: {len(salary_analytics)} salary records, "
                    f"{len(company_analytics)} company records"
                )
                
                return salary_analytics, company_analytics
                
        except Exception as e:
            self.logger.error(f"Error running analytics: {str(e)}")
            raise
            
    def get_batch_stats(self, days: int = 7) -> pd.DataFrame:
        """Get statistics for recent batch runs."""
        try:
            query = """
            SELECT 
                batch_id,
                start_time,
                end_time,
                total_jobs,
                new_jobs,
                updated_jobs,
                status,
                error_message
            FROM job_batches
            WHERE start_time >= datetime('now', ?)
            ORDER BY start_time DESC
            """
            
            with self.get_connection() as conn:
                stats = pd.read_sql(query, conn, params=(f'-{days} days',))
                self.logger.info(f"Retrieved stats for {len(stats)} batches from the last {days} days")
                return stats
                
        except Exception as e:
            self.logger.error(f"Error retrieving batch stats: {str(e)}")
            raise