import duckdb
import threading
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DuckDB-Concurrency-Test")

DB_FILE = 'telemetry.db'
TABLE_NAME = 'system_metrics'

def writer_process():
    """Simulates the telemetry collector process that writes to DuckDB"""
    logger.info("Starting writer process")
    for i in range(5):
        # Open and close connection for each write
        conn = None
        try:
            conn = duckdb.connect(database=DB_FILE, read_only=False)
            
            # Insert a test record
            timestamp = f"2025-05-07 18:{i}0:00"
            cpu_usage = 60 + i
            conn.execute(
                f"INSERT INTO {TABLE_NAME} VALUES ('{timestamp}', 'test-server', {cpu_usage}, 50.0, 40.0)"
            )
            logger.info(f"Writer: Inserted record with CPU={cpu_usage}%")
            
            # Close connection after write
            conn.close()
            conn = None
            logger.info("Writer: Connection closed after writing")
            
            time.sleep(1)  # Wait a second between writes
        except Exception as e:
            logger.error(f"Writer error: {e}")
        finally:
            if conn:
                conn.close()
                logger.info("Writer: Connection explicitly closed in finally block")

def reader_process():
    """Simulates the Streamlit app that reads from DuckDB"""
    logger.info("Starting reader process")
    for i in range(10):
        # Open connection with READ_ONLY access mode
        conn = None
        try:
            conn = duckdb.connect(database=DB_FILE, read_only=True, access_mode='READ_ONLY')
            
            # Run a query
            results = conn.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp DESC LIMIT 3").fetchall()
            logger.info(f"Reader: Query returned {len(results)} results")
            for row in results:
                logger.info(f"Reader: {row}")
            
            # Close connection after read
            conn.close()
            conn = None
            logger.info("Reader: Connection closed after reading")
            
            time.sleep(0.6)  # Read more frequently than writes
        except Exception as e:
            logger.error(f"Reader error: {e}")
        finally:
            if conn:
                conn.close()
                logger.info("Reader: Connection explicitly closed in finally block")

def main():
    """Test concurrent read/write access to DuckDB"""
    logger.info("Starting concurrency test")
    
    # First verify the table exists
    conn = duckdb.connect(database=DB_FILE, read_only=False)
    try:
        # Check if table exists
        table_check = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'").fetchone()
        if not table_check:
            logger.error(f"Table {TABLE_NAME} does not exist. Make sure telemetry_collector.py has been run first.")
            return
        logger.info(f"Table {TABLE_NAME} exists - proceeding with concurrency test")
    finally:
        conn.close()
    
    # Start the writer and reader threads
    writer = threading.Thread(target=writer_process)
    reader = threading.Thread(target=reader_process)
    
    writer.start()
    reader.start()
    
    writer.join()
    reader.join()
    
    logger.info("Concurrency test complete")

if __name__ == "__main__":
    main() 