#!/usr/bin/env python3
"""
Ageing Fact Table Processor using psycopg2
Processes invoices, credit notes, and payments to create ageing fact table
"""

import psycopg2
import logging
from datetime import date, datetime
from typing import Dict, List, Optional
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

class AgeingProcessor:
    """Main processor for ageing fact table generation"""
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            config = get_db_config()
            self.conn = psycopg2.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['user'],
                password=config['password']
            )
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def execute_migrations(self):
        """Execute migration files in order"""
        migration_files = [
            "migrations/01_invoices.sql",
            "migrations/02_credit_notes.sql", 
            "migrations/03_payments.sql",
            "migrations/04_ageing_fact_table.sql"
        ]
        
        # Check if all migration files exist before executing
        missing_files = []
        for file_path in migration_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            error_msg = f"Missing migration files: {', '.join(missing_files)}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        logger.info(f"All {len(migration_files)} migration files found, proceeding with execution")
        
        for file_path in migration_files:
            try:
                logger.info(f"Executing migration: {file_path}")
                with open(file_path, 'r') as f:
                    sql = f.read()
                
                with self.conn.cursor() as cur:
                    cur.execute(sql)
                self.conn.commit()
                logger.info(f"Migration {file_path} executed successfully")
                
            except Exception as e:
                logger.error(f"Failed to execute migration {file_path}: {e}")
                self.conn.rollback()
                raise
     
    def clear_existing_ageing_data(self, as_at_date: date):
        """Clear existing ageing data for the specified date"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ageing_fact_table WHERE as_at_date = %s",
                    (as_at_date,)
                )
            self.conn.commit()
            logger.info(f"Cleared existing ageing data for date: {as_at_date}")
        except Exception as e:
            logger.error(f"Failed to clear existing ageing data: {e}")
            self.conn.rollback()
            raise
    
    def generate_ageing_fact(self, as_at_date: date):
        """Generate ageing fact table data"""
        try:
            # Read SQL from file
            with open('sql/generate_ageing_fact.sql', 'r') as f:
                ageing_sql = f.read()
            
            with self.conn.cursor() as cur:
                cur.execute(ageing_sql, (as_at_date, as_at_date, as_at_date, as_at_date, as_at_date, as_at_date))
            self.conn.commit()
            logger.info(f"Ageing fact table generated successfully for date: {as_at_date}")
        except Exception as e:
            logger.error(f"Failed to generate ageing fact table: {e}")
            self.conn.rollback()
            raise
    

    
    def export_ageing_to_csv(self, as_at_date: date, filename: str = None):
        """Export ageing fact table data to CSV using COPY"""
        if filename is None:
            filename = f"ageing_fact_table_{as_at_date}.csv"
        
        copy_sql = f"""
        COPY (
            SELECT 
                centre_id,
                class_id,
                document_id,
                document_date,
                student_id,
                day_30,
                day_60,
                day_90,
                day_120,
                day_150,
                day_180,
                day_180_and_above,
                document_type,
                as_at_date
            FROM ageing_fact_table
            WHERE as_at_date = '{as_at_date}'
            ORDER BY centre_id, class_id, document_id
        ) TO STDOUT WITH CSV HEADER
        """
        
        try:
            with open(filename, 'w') as csvfile:
                with self.conn.cursor() as cur:
                    cur.copy_expert(copy_sql, csvfile)
            
            logger.info(f"Exported ageing fact table to: {filename}")
            print(f"✅ Exported ageing fact table to: {filename}")
                
        except Exception as e:
            logger.error(f"Failed to export ageing results: {e}")
            raise

def main():
    """Main function to run the ageing processor"""
    try:
        # Initialize processor
        processor = AgeingProcessor()
        
        # Connect to database
        processor.connect()
        
        # Execute migrations
        processor.execute_migrations()
        

        as_at_date = date(2025, 7, 7)
        logger.info(f"Processing ageing data for date: {as_at_date}")
                
        # Generate ageing fact table
        processor.generate_ageing_fact(as_at_date)
        
        # Export to CSV
        processor.export_ageing_to_csv(as_at_date)
        
        logger.info("Ageing processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Ageing processing failed: {e}")
        raise
    finally:
        if processor:
            processor.disconnect()

if __name__ == "__main__":
    main() 