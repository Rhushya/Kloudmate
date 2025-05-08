import duckdb
import psutil
import time
import datetime
import socket
import logging
import os

DB_FILE = 'telemetry.db'
TABLE_NAME = 'system_metrics'
COLLECTION_INTERVAL_SECONDS = 10
HOSTNAME = socket.gethostname()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

def create_table_if_not_exists(conn):
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                timestamp TIMESTAMP,
                hostname VARCHAR,
                cpu_usage FLOAT,
                memory_usage FLOAT,
                disk_usage FLOAT,
                UNIQUE(timestamp, hostname)
            )
        """)
        logger.info(f"Table '{TABLE_NAME}' ensured to exist.")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise

def get_system_metrics():
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory().percent
    try:
        disk = psutil.disk_usage('/').percent
    except Exception as e:
        logger.warning(f"Could not get disk usage for '/': {e}. Defaulting to 0.")
        disk = 0.0

    return {
        "timestamp": datetime.datetime.now(),
        "hostname": HOSTNAME,
        "cpu_usage": cpu,
        "memory_usage": memory,
        "disk_usage": disk
    }

def store_metrics(conn, metrics):
    try:
        conn.execute(
            f"INSERT INTO {TABLE_NAME} (timestamp, hostname, cpu_usage, memory_usage, disk_usage) VALUES (?, ?, ?, ?, ?)",
            (metrics["timestamp"], metrics["hostname"], metrics["cpu_usage"], metrics["memory_usage"], metrics["disk_usage"])
        )
        logger.info(f"Stored metrics: CPU={metrics['cpu_usage']:.2f}%, "
                    f"Mem={metrics['memory_usage']:.2f}%, Disk={metrics['disk_usage']:.2f}%")
    except duckdb.ConstraintException:
        logger.warning(f"Duplicate entry for timestamp {metrics['timestamp']} and host {metrics['hostname']}. Skipping.")
    except Exception as e:
        logger.error(f"Error storing metrics: {e}")

def main():
    logger.info("Starting telemetry collector...")
    while True:
        conn = None
        try:
            conn = duckdb.connect(database=DB_FILE, read_only=False)
            create_table_if_not_exists(conn)
            
            metrics = get_system_metrics()
            store_metrics(conn, metrics)
            
            conn.close()
            conn = None
            logger.debug("Connection closed after writing metrics")
            
            time.sleep(COLLECTION_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Telemetry collector stopped by user.")
            break
        except Exception as e:
            logger.error(f"Unhandled exception in telemetry collector: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()
                logger.info("DuckDB connection closed.")
                
    logger.info("Telemetry collector shutdown complete.")

if __name__ == "__main__":
    main() 