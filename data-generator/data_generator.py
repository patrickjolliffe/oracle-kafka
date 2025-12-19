import oracledb
import logging
import os
import time
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDataGenerator:
    def __init__(self):
        db_host = os.getenv('DATABASE_HOST', 'oracle')
        db_port = os.getenv('DATABASE_PORT', '1521')
        db_service = os.getenv('DATABASE_SERVICE_NAME', 'FREE')
        dsn = f"{db_host}:{db_port}/{db_service}"
        
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to Oracle at {dsn} (attempt {attempt + 1}/{max_retries})...")
                self.conn = oracledb.connect(
                    user='kafka',
                    password='kafka',
                    dsn=dsn
                )
                logger.info("Successfully connected to Oracle")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect after {max_retries} attempts")
                    raise
    
    def get_max_key(self):
        """Get current max deh_key"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(deh_key) FROM kafka.data_extraction_history")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result[0] else 0
    
    def insert_test_data(self):
        """Insert random test data"""
        cursor = self.conn.cursor()
        
        max_key = self.get_max_key()
        new_key = max_key + 1
        
        statuses = ['ACTIVE', 'PENDING', 'COMPLETED', 'FAILED']
        status = random.choice(statuses)
        name = f"Extract {new_key} {datetime.now().strftime('%H:%M:%S')}"
        
        try:
            cursor.execute(
                "INSERT INTO kafka.data_extraction_history (deh_key, deh_name, deh_status) VALUES (:1, :2, :3)",
                (new_key, name, status)
            )
            self.conn.commit()
            logger.info(f"Inserted: key={new_key}, name={name}, status={status}")
            cursor.close()
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            self.conn.rollback()
    
    def run(self):
        """Continuously generate data"""
        interval = int(os.getenv('INSERT_INTERVAL', '10'))  # Insert every 10 seconds
        
        try:
            while True:
                logger.info(f"Generating test data (interval: {interval}s)...")
                self.insert_test_data()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self.conn.close()

if __name__ == '__main__':
    generator = TestDataGenerator()
    generator.run()