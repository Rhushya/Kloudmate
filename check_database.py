import os
import duckdb
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'telemetry.db'
TABLE_NAME = 'system_metrics'

def check_database():
    """Check if the database exists and has data."""
    if not os.path.exists(DB_FILE):
        logger.warning(f"Database file '{DB_FILE}' does not exist! Please run telemetry_collector.py first.")
        return False
    
    try:
        # Connect in read-only mode
        conn = duckdb.connect(database=DB_FILE, read_only=True)
        
        # Check if the table exists
        result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not result:
            logger.warning(f"Table '{TABLE_NAME}' does not exist in the database! Please run telemetry_collector.py first.")
            conn.close()
            return False
        
        # Check if there is any data in the table
        count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        if count == 0:
            logger.warning(f"No data in table '{TABLE_NAME}'! Please run telemetry_collector.py for a few minutes to collect data.")
            conn.close()
            return False
        
        # Success if we get here
        logger.info(f"Database check successful: {count} records found in {TABLE_NAME}")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error checking database: {e}")
        return False

if __name__ == "__main__":
    if check_database():
        logger.info("The database is ready for querying!")
    else:
        logger.error("Please run telemetry_collector.py first to collect some data.") 