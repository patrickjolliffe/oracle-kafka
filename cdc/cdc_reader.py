import cx_Oracle
import json
import logging
import os
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
        
        # Oracle connection
        self.conn = cx_Oracle.connect(
            user='kafka',
            password='kafka',
            dsn=dsn
        )
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Connected to Oracle at {dsn}")
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
        
    def read_mlog(self):
        """Read changes from MLOG$ table"""
        cursor = self.conn.cursor()
        
        query = """
        SELECT 
            ROWID,
            DMLTYPE$$ as op_type,
            CHANGE_TIME$$ as change_time,
            OLD_NEW$$ as is_new
        FROM kafka.MLOG$_DATA_EXTRACTION_HISTORY
        ORDER BY CHANGE_TIME$$
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Found {len(rows)} changes")
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
    
    def run(self):
        """Main CDC process"""
        try:
            logger.info("Starting CDC process...")
            columns, rows = self.read_mlog()
            
            for row in rows:
                rowid_val = row[0]
                op_type = row[1]  # I, U, D
                
                # Get full row data
                row_data = self.get_current_row(rowid_val)
                
                if row_data is None and op_type != 'D':
                    continue
                
                # Build event
                event = {
                    'op': 'c' if op_type == 'I' else 'u' if op_type == 'U' else 'd',
                    'data': row_data,
                    'ts_ms': int(datetime.now().timestamp() * 1000)
                }
                
                self.producer.send('kafka.cdc.data_extraction_history', event)
                logger.info(f"Sent {op_type} event: {row_data}")
            
            logger.info("CDC cycle complete")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
        finally:
            self.producer.flush()
            self.producer.close()
            self.conn.close()

if __name__ == '__main__':
    cdc = SnapshotLogCDC()
    cdc.run()