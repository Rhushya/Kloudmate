import os
import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'telemetry.db'
TABLE_NAME = 'system_metrics'

def check_database():
    if not os.path.exists(DB_FILE):
        logger.warning(f"Database file '{DB_FILE}' does not exist! Please run telemetry_collector.py first.")
        return False
    
    try:
        conn = duckdb.connect(database=DB_FILE, read_only=True)
        
        result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not result:
            logger.warning(f"Table '{TABLE_NAME}' does not exist in the database! Please run telemetry_collector.py first.")
            conn.close()
            return False
        
        count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        if count == 0:
            logger.warning(f"No data in table '{TABLE_NAME}'! Please run telemetry_collector.py for a few minutes to collect data.")
            conn.close()
            return False
        
        logger.info(f"Database check successful: {count} records found in {TABLE_NAME}")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        return False

def show_database():
    if not os.path.exists(DB_FILE):
        logger.warning(f"Database file '{DB_FILE}' does not exist! Please run telemetry_collector.py first.")
        return None
    
    try:
        conn = duckdb.connect(database=DB_FILE, read_only=True)
        
        schema_info = conn.execute(f"DESCRIBE {TABLE_NAME}").fetchall()
        sample_data = conn.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp DESC LIMIT 10").fetchall()
        record_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        time_range = conn.execute(f"SELECT MIN(timestamp) as first_record, MAX(timestamp) as last_record FROM {TABLE_NAME}").fetchone()
        
        conn.close()
        
        # Convert datetime objects to strings
        first_record = str(time_range[0]) if time_range[0] is not None else None
        last_record = str(time_range[1]) if time_range[1] is not None else None
        
        return {
            "schema": schema_info,
            "sample_data": sample_data,
            "record_count": record_count,
            "first_record": first_record,
            "last_record": last_record
        }
        
    except Exception as e:
        logger.error(f"Error displaying database: {e}")
        return None

if __name__ == "__main__":
    if check_database():
        logger.info("The database is ready for querying!")
        
        db_info = show_database()
        if db_info:
            print("\nDatabase Information:")
            print(f"Total Records: {db_info['record_count']}")
            if db_info['first_record']:
                print(f"Time Range: {db_info['first_record']} to {db_info['last_record']}")
            
            print("\nSchema:")
            for col in db_info['schema']:
                print(f"- {col[0]}: {col[1]}")
            
            print("\nSample Data:")
            for row in db_info['sample_data'][:5]:
                print(row)
    else:
        logger.error("Please run telemetry_collector.py first to collect some data.") 