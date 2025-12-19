import oracledb
import json
import logging
import os
import time
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnapshotLogCDC:
    def __init__(self):
        # Get database config from environment
        db_host = os.getenv('DATABASE_HOST', 'oracle')
        db_port = os.getenv('DATABASE_PORT', '1521')
        db_service = os.getenv('DATABASE_SERVICE_NAME', 'FREE')
        dsn = f"{db_host}:{db_port}/{db_service}"
        
        # Get Kafka config from environment
        kafka_host = os.getenv('KAFKA_HOST', 'kafka')
        kafka_port = os.getenv('KAFKA_PORT', '9092')
        bootstrap_servers = [f"{kafka_host}:{kafka_port}"]
        
        # Retry Oracle connection
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
                logger.info(f"Successfully connected to Oracle")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect after {max_retries} attempts")
                    raise
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
        
    def check_snapshot_done(self):
        """Check if initial snapshot has been completed"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT state_value FROM kafka.cdc_state WHERE state_key = 'snapshot_done'")
            result = cursor.fetchone()
            cursor.close()
            return result and result[0] == 'true'
        except Exception as e:
            logger.warning(f"Failed to check snapshot state: {e}. Assuming not done.")
            return False
    
    def mark_snapshot_done(self):
        """Mark initial snapshot as complete"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE kafka.cdc_state SET state_value = 'true', updated_at = SYSDATE WHERE state_key = 'snapshot_done'")
            self.conn.commit()
            cursor.close()
            logger.info("Marked snapshot as complete")
        except Exception as e:
            logger.error(f"Failed to mark snapshot done: {e}")
            self.conn.rollback()
    
    def read_table_snapshot(self):
        """Read all rows from table for initial bootstrap"""
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM kafka.data_extraction_history ORDER BY deh_key"
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Found {len(rows)} rows in table snapshot")
        return columns, rows
    
    def read_mlog(self):
        """Read changes from MLOG$ table"""
        cursor = self.conn.cursor()
        
        query = """
        SELECT 
            ROWID,
            DMLTYPE$$ as op_type,
            SNAPTIME$$ as change_time,
            OLD_NEW$$ as is_new
        FROM kafka.MLOG$_DATA_EXTRACTION_HISTORY
        ORDER BY SNAPTIME$$
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Found {len(rows)} changes in MLOG$")
        return columns, rows
    
    def get_current_row(self, rowid_val):
        """Get current row data"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM kafka.data_extraction_history WHERE ROWID = :rid",
            rid=rowid_val
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        cursor.close()
        
        if row:
            return dict(zip(columns, row))
        return None
    
    def purge_old_logs(self):
        """Purge snapshot logs older than 24 hours"""
        try:
            logger.info("Purging snapshot logs older than 24 hours...")
            cursor = self.conn.cursor()
            
            # Purge data_extraction_history logs
            cursor.callproc('DBMS_SNAPSHOT.PURGE_LOG', [
                'kafka',
                'data_extraction_history'
            ])
            logger.info("Purged data_extraction_history logs")
            
            # Purge debug_data logs
            cursor.callproc('DBMS_SNAPSHOT.PURGE_LOG', [
                'kafka',
                'debug_data'
            ])
            logger.info("Purged debug_data logs")
            
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Purge failed (may be expected): {e}")
            self.conn.rollback()
    
    def _convert_datetime(self, data):
        """Convert datetime objects to ISO format strings"""
        if isinstance(data, dict):
            return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in data.items()}
        return data
    
    def run_cycle(self):
        """Single CDC cycle (doesn't close connections)"""
        try:
            snapshot_done = self.check_snapshot_done()
            
            if not snapshot_done:
                logger.info("Running initial snapshot of table...")
                columns, rows = self.read_table_snapshot()
                
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    row_dict = self._convert_datetime(row_dict)
                    
                    event = {
                        'op': 'r',  # read/initial snapshot
                        'data': row_dict,
                        'ts_ms': int(datetime.now().timestamp() * 1000)
                    }
                    
                    self.producer.send('kafka.cdc.data_extraction_history', event)
                    logger.info(f"Sent initial snapshot event: {row_dict}")
                
                self.mark_snapshot_done()
                logger.info("Initial snapshot complete, switching to change tracking...")
            else:
                logger.info("Reading incremental changes from MLOG$...")
                columns, rows = self.read_mlog()
                
                for row in rows:
                    rowid_val = row[0]
                    op_type = row[1]  # I, U, D
                    
                    row_data = self.get_current_row(rowid_val)
                    
                    if row_data is None and op_type != 'D':
                        continue
                    
                    row_data = self._convert_datetime(row_data)
                    
                    event = {
                        'op': 'c' if op_type == 'I' else 'u' if op_type == 'U' else 'd',
                        'data': row_data,
                        'ts_ms': int(datetime.now().timestamp() * 1000)
                    }
                    
                    self.producer.send('kafka.cdc.data_extraction_history', event)
                    logger.info(f"Sent {op_type} event: {row_data}")
                
                # Purge old logs after reading
                self.purge_old_logs()
            
            logger.info("CDC cycle complete")
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
    
    def close(self):
        """Close connections"""
        logger.info("Closing connections...")
        self.producer.close()
        self.conn.close()

if __name__ == '__main__':
    cdc = SnapshotLogCDC()
    poll_interval = 60  # Poll every minute
    
    try:
        while True:
            cdc.run_cycle()
            logger.info(f"Waiting {poll_interval}s before next cycle...")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
    finally:
        cdc.close()