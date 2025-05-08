# RAG-based Observability Assistant

A Python application that uses a Retrieval-Augmented Generation (RAG) approach to query system telemetry data using natural language. The application collects CPU, memory, and disk usage metrics and stores them in DuckDB. It then uses natural language processing via a local LLM (Llama2) to translate user queries into SQL and provide human-readable insights.

## Features

- **Natural Language Queries**: Ask questions about system metrics in plain English
- **Telemetry Collection**: Automatic collection of CPU, memory, and disk usage data
- **Columnar Database**: Fast storage in DuckDB
- **Local LLM**: Powered by Llama2 via Ollama
- **Web UI**: Interactive Streamlit interface for queries and results
- **Concurrent Access**: Optimized for concurrent read/write access to database

## Prerequisites

1. **Python 3.8+**
2. **Ollama**:
   - Download and install Ollama from [https://ollama.ai/](https://ollama.ai/)
   - Pull the Llama2 model: `ollama pull llama2:7b`
   - Ensure Ollama is running in the background (default: http://localhost:11434)

## Installation

1. Clone this repository:
   ```
   git clone <your-repo-url>
   cd observability_assistant
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure (optional):
   - The `.env` file contains configuration for Ollama. By default, it uses `http://localhost:11434`.
   - You can modify collection intervals in `telemetry_collector.py` if needed.

## Usage

1. **Start the Telemetry Collector**:
   ```
   python telemetry_collector.py
   ```
   This will begin collecting system metrics and storing them in `telemetry.db`.
   Let it run for a few minutes to collect meaningful data.

2. **Start the Streamlit App**:
   ```
   streamlit run app.py
   ```
   This will open the application in your web browser.

3. **Ask Questions**:
   - "Which servers had >65% memory usage in the last 24 hours?"
   - "Did any service spike over 85% CPU last week?"
   - "List hosts with >90% disk usage in the past 12 hours"
   - "Show me CPU usage for the last 10 minutes"
   - "Any high memory usage recently?"

## How It Works

1. The telemetry collector (`telemetry_collector.py`) uses `psutil` to gather system metrics.
2. Metrics are stored in DuckDB with schema: `(timestamp, hostname, cpu_usage, memory_usage, disk_usage)`.
3. When you ask a question:
   - The LLM converts your natural language query to SQL
   - The SQL query runs against DuckDB
   - The LLM summarizes the results into human-readable insights
   - The answer is displayed in the Streamlit UI

## Database Concurrency

The application uses DuckDB's concurrency features to allow simultaneous read/write access:

1. The **telemetry collector** opens and closes database connections for each metrics collection cycle
2. The **Streamlit app** uses READ_ONLY access mode to safely query the database while the collector is writing
3. This approach prevents "File is already open" errors that can occur with concurrent access

If you want to test the concurrency model, you can run:
```
python test_concurrency.py
```
This simulates simultaneous read/write operations to verify the solution works correctly.

## Project Structure

```
observability_assistant/
├── telemetry_collector.py  # Collects and stores psutil data into DuckDB
├── app.py                  # Streamlit application (RAG logic)
├── .env                    # For OLLAMA_BASE_URL configuration
├── requirements.txt        # Python dependencies
├── test_concurrency.py     # Test script for DB concurrency
└── README.md               # This file
```

## Limitations

- The application is designed for collecting metrics from the local machine. For a multi-host setup, additional configuration would be required.
- The current schema is simple (timestamp, hostname, cpu_usage, memory_usage, disk_usage). For more complex telemetry, the schema and query generation would need to be expanded.
- The LLM runs locally through Ollama, which might have performance implications on resource-constrained systems.

## License

MIT 