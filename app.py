import streamlit as st
import duckdb
import logging
import datetime
import os
from dotenv import load_dotenv

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - StreamlitApp - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Try to import from langchain_ollama first (recommended), but fall back to langchain_community if not available
try:
    from langchain_ollama import Ollama as OllamaLLM
    logger.info("Using langchain_ollama package for Ollama integration")
except ImportError:
    # Fall back to the older import method
    from langchain_community.llms import Ollama as OllamaLLM
    logger.info("Using langchain_community package for Ollama integration (legacy)")

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSequence

# --- Configuration & Setup ---
load_dotenv() # Load .env file if present
DB_FILE = 'telemetry.db'
TABLE_NAME = 'system_metrics' # Must match telemetry_collector.py
OLLAMA_MODEL = 'llama2:7b' # Make sure this model is pulled in Ollama

# --- LLM and LangChain Setup ---
try:
    ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    # Update to use the new OllamaLLM class from langchain_ollama
    llm = OllamaLLM(model=OLLAMA_MODEL, base_url=ollama_base_url)
    logger.info(f"Ollama LLM initialized with model {OLLAMA_MODEL} at {ollama_base_url}")
except Exception as e:
    logger.error(f"Failed to initialize Ollama LLM: {e}")
    st.error(f"Failed to initialize Ollama LLM: {e}. Ensure Ollama is running and the model '{OLLAMA_MODEL}' is available.")
    st.stop()


# --- Database Functions ---
@st.cache_resource # Cache the connection across Streamlit reruns
def get_db_connection():
    try:
        # Use only read_only=True without access_mode parameter 
        # This works across different DuckDB versions
        conn = duckdb.connect(database=DB_FILE, read_only=True)
        logger.info(f"Successfully connected to DuckDB database: {DB_FILE}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        st.error(f"Error connecting to database: {e}")
        return None

def execute_query(conn, query):
    """Executes a SQL query and returns the results."""
    if conn is None:
        return None, "Database connection not available."
    try:
        logger.info(f"Executing SQL query: {query}")
        results = conn.execute(query).fetchall()
        logger.info(f"Query results count: {len(results)}")
        # For large results, log only a summary or first few
        if results:
            logger.debug(f"First result row: {results[0]}")
        return results, None
    except Exception as e:
        logger.error(f"Error executing SQL query '{query}': {e}")
        return None, f"Error executing SQL query: {e}"

# --- Prompt Templates ---
# Note: DuckDB uses standard SQL timestamp/interval functions.
# NOW() or current_timestamp, INTERVAL 'X' UNIT (e.g., INTERVAL '24 hours')

SCHEMA_GUIDANCE = f"""
You are an AI assistant that converts natural language queries into SQL queries for a DuckDB database.
The database table is named '{TABLE_NAME}' and has the following columns:
- timestamp (TIMESTAMP): The time of the metric collection.
- hostname (VARCHAR): The name of the server/host.
- cpu_usage (FLOAT): CPU utilization percentage (0-100).
- memory_usage (FLOAT): Memory utilization percentage (0-100).
- disk_usage (FLOAT): Disk utilization percentage (0-100).

Guidelines for SQL generation:
1. Always use the table name '{TABLE_NAME}'.
2. For time-based queries:
   - "last 24 hours": `timestamp >= NOW() - INTERVAL '24 hours'`
   - "last 7 days" or "last week": `timestamp >= NOW() - INTERVAL '7 days'`
   - "last hour": `timestamp >= NOW() - INTERVAL '1 hour'`
   - "past 12 hours": `timestamp >= NOW() - INTERVAL '12 hours'`
   - For other specific time ranges, adapt accordingly.
3. Map natural language metrics to column names:
   - "CPU" or "cpu usage" -> `cpu_usage`
   - "memory" or "memory usage" -> `memory_usage`
   - "disk" or "disk usage" -> `disk_usage`
4. For threshold queries (e.g., "memory usage > 65%"), use the correct column and comparison operator.
   The metric values are percentages, so if the user says "65%", use `65` in the SQL.
5. SELECT all columns (`SELECT *`) or specific relevant columns like `hostname, timestamp, cpu_usage` etc.
   If the query asks for "servers" or "hosts", ensure `hostname` is selected.
6. If the query asks for a list or specific instances, `SELECT DISTINCT hostname, timestamp, <metric_column>` is often appropriate.
7. If the query asks "Did any service spike...", you might want to use `COUNT(*)` or select specific instances.

Example Natural Language Query: "Show me servers that crossed 65% memory usage in the past 24 hours."
Example SQL Output: SELECT DISTINCT hostname, timestamp, memory_usage FROM {TABLE_NAME} WHERE memory_usage > 65 AND timestamp >= NOW() - INTERVAL '24 hours' ORDER BY timestamp DESC;

Example Natural Language Query: "Did any service spike over 85% CPU last week?"
Example SQL Output: SELECT hostname, timestamp, cpu_usage FROM {TABLE_NAME} WHERE cpu_usage > 85 AND timestamp >= NOW() - INTERVAL '7 days' ORDER BY timestamp DESC LIMIT 10;

Example Natural Language Query: "List hosts with >90% disk usage in the past 12 hours"
Example SQL Output: SELECT DISTINCT hostname, timestamp, disk_usage FROM {TABLE_NAME} WHERE disk_usage > 90 AND timestamp >= NOW() - INTERVAL '12 hours' ORDER BY hostname, timestamp DESC;

Only output the SQL query. Do not add any other text, explanation, or markdown formatting.
"""

nl_to_sql_prompt_template = PromptTemplate(
    input_variables=["natural_language_query", "schema_guidance"],
    template="""{schema_guidance}

Natural Language Query: {natural_language_query}
SQL Query:"""
)

summarize_results_prompt_template = PromptTemplate(
    input_variables=["natural_language_query", "sql_query", "sql_results"],
    template="""You are an AI assistant that summarizes database query results in a human-readable way.
Original Natural Language Query: {natural_language_query}
Generated SQL Query: {sql_query}
SQL Query Results:
{sql_results}

Based on the query and results, provide a concise, natural language summary.
If the results are empty, state that no data matched the criteria.
If there are many results, summarize the key findings rather than listing everything.
Focus on answering the original question.
For example, if the query was "Which servers had >65% memory usage?", your summary could be:
"The following servers had memory usage over 65% in the specified period: server-A (70% at YYYY-MM-DD HH:MM), server-B (80% at YYYY-MM-DD HH:MM)."
Or, if no data: "No servers were found with memory usage over 65% in the specified period."

Summary:"""
)

# --- LangChain Chains ---
# Replace from_components with the correct syntax for RunnableSequence
sql_generation_chain = (
    {
        "natural_language_query": lambda x: x["natural_language_query"], 
        "schema_guidance": lambda x: x["schema_guidance"]
    } 
    | nl_to_sql_prompt_template 
    | llm 
    | StrOutputParser()
)

summarization_chain = (
    {
        "natural_language_query": lambda x: x["natural_language_query"],
        "sql_query": lambda x: x["sql_query"],
        "sql_results": lambda x: x["sql_results"]
    } 
    | summarize_results_prompt_template 
    | llm 
    | StrOutputParser()
)


# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Observability Assistant")
st.title("📈 RAG-based Observability Assistant")
st.markdown("Ask questions about your system's telemetry data (CPU, Memory, Disk). Ensure `telemetry_collector.py` is running.")

# Initialize session state for conversation history (optional, but good for context)
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message:
            with st.expander("Generated SQL Query"):
                st.code(message["sql"], language="sql")
        if "raw_results" in message and message["raw_results"] is not None: # Check if raw_results exist and are not None
             with st.expander("Raw Query Results"):
                st.dataframe(message["raw_results"])


# User input
user_query = st.chat_input("e.g., Which servers had >65% memory usage in the last 24h?")

if user_query:
    logger.info(f"User query: {user_query}")
    st.session_state.messages.append({"role": "user", "content": user_query})
    with st.chat_message("user"):
        st.markdown(user_query)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        thinking_message = "🤔 Thinking and processing your query..."
        message_placeholder.markdown(thinking_message)

        conn = get_db_connection()
        if conn is None:
            error_msg = "Database connection failed. Cannot process query."
            logger.error(error_msg)
            message_placeholder.error(error_msg)
            st.session_state.messages.append({"role": "assistant", "content": error_msg})
            st.stop()

        generated_sql = ""
        sql_results = None
        error_in_processing = None

        try:
            # 1. Convert NL to SQL
            with st.spinner("Generating SQL query..."):
                logger.info("Attempting to generate SQL query...")
                # Provide schema guidance dynamically to the chain
                sql_generation_input = {"natural_language_query": user_query, "schema_guidance": SCHEMA_GUIDANCE}
                generated_sql = sql_generation_chain.invoke(sql_generation_input)
                # Basic cleaning: remove potential markdown backticks if LLM adds them
                generated_sql = generated_sql.strip().replace("```sql", "").replace("```", "").strip()
                logger.info(f"Generated SQL: {generated_sql}")
                message_placeholder.markdown(thinking_message + f"\n\n🔍 Generated SQL:\n```sql\n{generated_sql}\n```")


            # 2. Execute SQL Query
            with st.spinner("Executing SQL query on DuckDB..."):
                logger.info("Attempting to execute SQL query...")
                # Convert to DataFrame for better display and potential summarization inputs
                results_list, db_error = execute_query(conn, generated_sql)
                if db_error:
                    error_in_processing = f"Database error: {db_error}"
                    logger.error(error_in_processing)
                elif results_list is not None:
                    # Convert list of tuples to a more structured format for the LLM if needed,
                    # or pass as string. For now, converting to string for the prompt.
                    if results_list:
                        # Get column names from the cursor description if available
                        # This is a bit tricky as execute_query only returns fetchall()
                        # A more robust way would be to get cursor.description after execution
                        # For simplicity, we'll format it as a list of tuples string.
                        sql_results_str = "\n".join([str(row) for row in results_list[:20]]) # Limit for prompt
                        if len(results_list) > 20:
                            sql_results_str += f"\n... and {len(results_list) - 20} more rows."
                        logger.info(f"SQL results (first 20 rows or less): {sql_results_str}")
                        sql_results = results_list # Store the raw list for potential display
                    else:
                        sql_results_str = "No results found."
                        logger.info("SQL query returned no results.")
                        sql_results = [] # Store empty list

            # 3. Summarize Results with LLM
            if not error_in_processing:
                with st.spinner("Summarizing results..."):
                    logger.info("Attempting to summarize results...")
                    summarization_input = {
                        "natural_language_query": user_query,
                        "sql_query": generated_sql,
                        "sql_results": sql_results_str
                    }
                    summary = summarization_chain.invoke(summarization_input)
                    logger.info(f"LLM Summary: {summary}")
                    message_placeholder.markdown(summary)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": summary,
                        "sql": generated_sql,
                        "raw_results": sql_results
                    })
            else: # Handle error from SQL generation or execution
                message_placeholder.error(error_in_processing)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Sorry, I encountered an error: {error_in_processing}",
                    "sql": generated_sql if generated_sql else "N/A"
                })

        except Exception as e:
            error_msg = f"An unexpected error occurred: {e}"
            logger.error(error_msg, exc_info=True)
            message_placeholder.error(error_msg)
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Sorry, I encountered an unexpected error processing your request: {str(e)}",
                "sql": generated_sql if generated_sql else "N/A"
            }) 