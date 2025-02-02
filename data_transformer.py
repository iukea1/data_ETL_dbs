"""Data transformation operations for the job scraper ETL pipeline."""

import pandas as pd
import numpy as np
from typing import Optional, Tuple
import logging
from datetime import datetime
from config import REQUIRED_COLUMNS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataTransformer:
    def __init__(self):
        """Initialize the data transformer."""
        self.logger = logging.getLogger(__name__)
    
    @staticmethod
    def generate_job_id(row: pd.Series) -> str:
        """Generate a unique job ID based on job details."""
        components = [
            str(row.get('job_url', '')),
            str(row.get('company', '')),
            str(row.get('title', '')),
            str(row.get('location_city', '')),
            str(row.get('date_posted', ''))
        ]
        return '_'.join(filter(None, components))
    
    def extract_location(self, location: str) -> tuple[Optional[str], Optional[str]]:
        """Extract city and state from location string."""
        if pd.isna(location):
            return None, None
        
        try:
            parts = str(location).split(',')
            city = parts[0].strip() if len(parts) > 0 else None
            state = parts[1].strip() if len(parts) > 1 else None
            return city, state
        except Exception as e:
            self.logger.warning(f"Error extracting location from {location}: {str(e)}")
            return None, None
    
    def clean_salary_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize salary data."""
        try:
            # Convert salary columns to numeric, handling any non-numeric values
            df['salary_min_amount'] = pd.to_numeric(df['min_amount'], errors='coerce')
            df['salary_max_amount'] = pd.to_numeric(df['max_amount'], errors='coerce')
            
            # Set salary interval if not present
            if 'salary_interval' not in df.columns:
                df['salary_interval'] = df['interval']
            
            # Set salary currency if not present
            if 'salary_currency' not in df.columns:
                df['salary_currency'] = 'USD'
                
            # Ensure salary source is present
            if 'salary_source' not in df.columns:
                df['salary_source'] = df['source_site']
                
            return df
        except Exception as e:
            self.logger.error(f"Error cleaning salary data: {str(e)}")
            return df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean the scraped data."""
        if df is None or df.empty:
            self.logger.warning("Empty DataFrame received for transformation")
            return pd.DataFrame(columns=list(REQUIRED_COLUMNS.values()))
            
        try:
            # Handle location fields
            if 'location' in df.columns:
                df[['city', 'state']] = pd.DataFrame(
                    df['location'].apply(self.extract_location).tolist(),
                    index=df.index
                )
            
            # Set default country if not present
            if 'country' not in df.columns:
                df['country'] = 'USA'
            
            # Clean salary data
            df = self.clean_salary_data(df)
            
            # Clean dates
            df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
            df['scrape_date'] = pd.to_datetime(df['scrape_date'])
            
            # Generate job IDs
            df['job_id'] = df.apply(self.generate_job_id, axis=1)
            
            # Ensure all required columns exist with appropriate defaults
            for source_col, target_col in REQUIRED_COLUMNS.items():
                if target_col not in df.columns:
                    if source_col in df.columns:
                        df[target_col] = df[source_col]
                    else:
                        df[target_col] = None
                        
            # Convert boolean fields
            if 'is_remote' in df.columns:
                df['is_remote'] = df['is_remote'].fillna(False).astype(bool)
            
            # Select and return only the required columns
            result_df = df[list(REQUIRED_COLUMNS.values())].copy()
            
            # Log transformation summary
            self.logger.info(f"Transformed {len(result_df)} jobs with {len(result_df.columns)} columns")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in transform_data: {str(e)}")
            self.logger.debug(f"Available columns: {df.columns.tolist()}")
            raise
    
    def deduplicate_jobs(self, df: pd.DataFrame, existing_jobs: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate jobs from the current batch and against existing jobs."""
        if df.empty:
            return df
            
        try:
            # Remove duplicates within the current DataFrame
            df = df.drop_duplicates(subset=['job_url', 'company', 'title'])
            
            if not existing_jobs.empty:
                # Merge with existing jobs and keep only new ones
                merged = df.merge(
                    existing_jobs[['job_url', 'company', 'title']],
                    on=['job_url', 'company', 'title'],
                    how='left',
                    indicator=True
                )
                df = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
            
            self.logger.info(f"After deduplication: {len(df)} unique jobs remaining")
            return df
            
        except Exception as e:
            self.logger.error(f"Error in deduplicate_jobs: {str(e)}")
            raise