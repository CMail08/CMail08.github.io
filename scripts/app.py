# app.py
# Flask backend with LLM integration for natural language querying.
# Uses synchronous Gemini API calls. Reads API key from file.

import os
import sys
import logging
from flask import Flask, request, jsonify, send_from_directory
import psycopg2
# from psycopg2 import sql # Not used
import json
import decimal
from datetime import date, datetime, timedelta
import google.generativeai as genai
from pathlib import Path
import re
import textwrap # ADDED textwrap import
# import asyncio # REMOVED asyncio import

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Load Database Config ---
try:
    from database_config import get_connection_string
except ImportError:
    logger.error("ERROR: database_config.py not found.")
    sys.exit("database_config.py not found. Please ensure it's in the same directory as app.py.")

# --- Load Schema ---
SCHEMA_INFO = ""
try:
    # Assumes schema.sql is in the same directory as this script (scripts/)
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    with open(schema_path, 'r', encoding='utf-8') as f:
        SCHEMA_INFO = f.read()
    logger.info(f"Successfully loaded schema from {schema_path}")
except FileNotFoundError:
    logger.error(f"ERROR: schema.sql not found at {schema_path}. Schema awareness will fail.")
except Exception as e:
    logger.error(f"ERROR: Failed to read schema.sql: {e}")
    SCHEMA_INFO = "" # Ensure it's defined

# --- Load Distinct Album Names ---
ALBUM_LIST_INFO = ""
try:
    logger.info("Attempting to load distinct album names from database...")
    conn_str = get_connection_string()
    db_conn = psycopg2.connect(conn_str)
    cursor = db_conn.cursor()
    cursor.execute("SELECT DISTINCT album FROM songs WHERE album IS NOT NULL ORDER BY album;")
    albums = [row[0] for row in cursor.fetchall()]
    if albums:
        ALBUM_LIST_INFO = ", ".join(albums)
        logger.info(f"Successfully loaded {len(albums)} distinct album names.")
    else:
        logger.warning("No distinct album names found in the database.")
    cursor.close()
    db_conn.close()
except Exception as e:
    logger.error(f"ERROR: Failed to load distinct album names from database: {e}")
    # Ensure ALBUM_LIST_INFO remains empty or default
    ALBUM_LIST_INFO = "(Error loading album list)"

# --- Configure LLM ---
llm_model = None
gemini_api_key = None
try:
    # Reads key from file in the same directory as app.py
    api_key_file_path = Path(__file__).resolve().parent / "Gemmini 2.5 exp API.txt"
    logger.info(f"Attempting to load Gemini API key from: {api_key_file_path}")
    if not api_key_file_path.is_file():
        raise FileNotFoundError(f"API key file not found at the specified path.")
    with open(api_key_file_path, "r", encoding="utf-8") as f:
        gemini_api_key = f.read().strip()
    if not gemini_api_key:
        raise ValueError("API key file is empty.")
    logger.info(f"Successfully loaded API key from {api_key_file_path.name}")
    genai.configure(api_key=gemini_api_key)
    # Use model name from config.py if imported, otherwise default
    try:
        from config import LLM_MODEL_NAME
    except ImportError:
        LLM_MODEL_NAME = 'gemini-1.5-flash-latest' # Fallback
        logger.warning("Could not import LLM_MODEL_NAME from config, using default.")

    llm_model = genai.GenerativeModel(LLM_MODEL_NAME)
    logger.info(f"Configured Gemini model: {LLM_MODEL_NAME}")

except FileNotFoundError as e: logger.critical(f"API Key Error: {e}. LLM features will be disabled.")
except ValueError as e: logger.critical(f"API Key Error: {e}. LLM features will be disabled.")
except Exception as e: logger.error(f"LLM Configuration Error: {e}")

# Initialize Flask app
# Assumes frontend files are in ../frontend relative to this script's location (scripts/)
app = Flask(__name__, template_folder='../frontend', static_folder='../frontend')

# --- Helper Function for JSON Serialization ---
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, timedelta):
        return str(obj)
    raise TypeError (f"Type {type(obj)} not serializable")

# --- Helper Function for Formatting Explanation ---
def format_explanation_as_bullets(explanation_text):
    """Formats plain text explanation into an HTML bulleted list."""
    if not explanation_text:
        return ""
    # Simple sentence split based on common terminators followed by space or end of string
    # Handles basic cases, might need refinement for complex sentences.
    sentences = re.split(r'(?<=[.?!])\s+', explanation_text.strip())
    # Filter out empty strings that might result from splitting
    sentences = [s for s in sentences if s]
    if not sentences:
        # Add larger margin (1rem) to fallback list item too
        return f"<ul><li class=\"mb-4\">{explanation_text}</li></ul>"

    # Add mb-4 class for a 1rem margin-bottom to each list item
    list_items = ''.join([f"<li class=\"mb-4\">{s.strip()}</li>" for s in sentences])
    return f"<ul class=\"list-disc list-inside text-sm text-gray-700\">{list_items}</ul>"

# --- Helper Function for Wrapping SQL Comments ---
def wrap_sql_comments(sql_string, width=56):
    """
    Wraps SQL comments (lines starting with '--') to a specified width.
    Other lines are left untouched.
    """
    wrapped_lines = []
    if not sql_string: # Handle empty or None input
        return sql_string
    for line in sql_string.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith('--'):
            # It's a comment
            if len(stripped_line) > width:
                # Find prefix ('--' or '-- ')
                prefix = '-- '
                if not stripped_line.startswith(prefix):
                    prefix = '--'
                comment_text = stripped_line[len(prefix):]
                # Calculate available width for textwrap
                wrap_width = width - len(prefix)
                # Wrap the comment text
                wrapped_comment_lines = textwrap.wrap(comment_text, width=max(1, wrap_width))
                # Add prefix back to each wrapped line, preserving original indent if any
                leading_whitespace = line[:line.find('--')]
                for wrapped_line in wrapped_comment_lines:
                    wrapped_lines.append(leading_whitespace + prefix + wrapped_line)
            else:
                # Comment is short enough, keep original line
                wrapped_lines.append(line)
        else:
            # Not a comment, keep original line
            wrapped_lines.append(line)
    return '\n'.join(wrapped_lines)

# --- Helper Function for LLM Calls (Synchronous) ---
def generate_llm_response(prompt):
    """Sends prompt to configured LLM and returns text response or error."""
    if not llm_model:
        return None, "LLM not configured or key missing/invalid."
    try:
        logger.info(f"Sending prompt to LLM (first 100 chars): {prompt[:100]}...")
        # Use the SYNCHRONOUS generate_content method
        response = llm_model.generate_content(prompt)
        logger.info("Received response from LLM.")

        # Check for blocked response or empty parts
        # Accessing feedback might differ slightly in sync response, adjust if needed based on library docs
        if not response.parts and hasattr(response, 'prompt_feedback') and response.prompt_feedback.block_reason:
             block_reason = response.prompt_feedback.block_reason or "Unknown"
             logger.warning(f"LLM call blocked: {block_reason}.")
             return None, f"Request blocked by API: {block_reason}."
        # Check response.text directly
        if hasattr(response, 'text') and response.text:
             return response.text, None
        else: # Fallback to checking candidates
             try:
                 # Accessing candidates might differ slightly in sync response
                 if response.candidates and response.candidates[0].content.parts and hasattr(response.candidates[0].content.parts[0], 'text'):
                      logger.info("Using text from candidate parts.")
                      return response.candidates[0].content.parts[0].text, None
                 else: raise AttributeError("No suitable content")
             except (AttributeError, IndexError):
                  feedback_info = getattr(response, 'prompt_feedback', 'N/A')
                  logger.error(f"LLM empty/unparseable response. Feedback: {feedback_info}")
                  return None, "LLM returned empty or unparseable response."
    except Exception as e:
        logger.error(f"LLM API Error: {e}", exc_info=True)
        return None, f"LLM Error: {str(e)}"

# --- Routes ---
@app.route('/')
def index():
    # Serves the main index.html file from the frontend folder
    try:
        return send_from_directory('../frontend', 'index.html')
    except Exception as e:
        logger.error(f"Error serving index.html: {e}")
        return "Error loading page.", 500

@app.route('/<path:filename>')
def serve_static(filename):
    # Serves static files like script.js from the frontend folder
    return send_from_directory(app.static_folder, filename)

# --- Combined LLM Endpoint ---
@app.route('/process_nl_query', methods=['POST'])
def handle_process_nl_query():
    """Receives NL query, asks LLM for Formatted SQL and Explanation."""
    data = request.get_json()
    nl_query = data.get('query', '')
    if not nl_query: return jsonify({"error": "No query."}), 400
    if not SCHEMA_INFO: return jsonify({"error": "Schema missing."}), 500
    if not llm_model: return jsonify({"error": "LLM not configured."}), 500

    # Prompt asking for both SQL and Explanation, with specific formatting and typo tolerance
    # Adjusted f-string formatting (removed surrounding newlines)
    prompt = f"""You are an assistant helping users query a PostgreSQL database about Bruce Springsteen setlists.
Based ONLY on the provided database schema and the user's question, perform the following two tasks:
1. Generate a single, valid PostgreSQL SELECT query to retrieve the necessary data. Format the SQL query with standard indentation and line breaks for readability. Enclose the formatted SQL query within ```sql ... ```. Only generate SELECT statements. Prioritize using the get_stats_for_show_ids function if the user asks for subset statistics based on specific shows (pass an array of show_ids).
2. Provide a brief, user-friendly explanation (2-3 sentences) of what the generated query does, suitable for someone unfamiliar with SQL. Enclose the explanation within ```explanation ... ```.

VERY IMPORTANT: Your primary goal is to understand the user's INTENT. Be **extremely robust and generous** in matching user input (like song titles, album names, tours, locations) to the values suggested by the database schema and your general knowledge of the artist. **Aggressively correct** potential spelling errors, typos, variations in punctuation, or slightly different phrasings. If a user mentions 'Born To Run' or 'Born To Run album', you MUST interpret it as the album 'Born to Run'. If they mention 'Asbury Park', assume they mean 'Greetings From Asbury Park, N.J.' unless context strongly implies otherwise. **Prioritize finding a plausible match over failing due to minor discrepancies.**

DATABASE SCHEMA:
--- START SCHEMA ---
{SCHEMA_INFO}
--- END SCHEMA ---

KNOWN ALBUM NAMES IN DATABASE:
{ALBUM_LIST_INFO}

USER QUESTION:
"{nl_query}"

OUTPUT:"""
    llm_response_text, error = generate_llm_response(prompt) # Use sync helper

    if error:
        return jsonify({"error": error}), 500
    else:
        # Extract SQL
        sql_match = re.search(r"```sql\s*([\s\S]*?)\s*```", llm_response_text, re.IGNORECASE | re.DOTALL)
        extracted_sql = ""
        if sql_match:
             extracted_sql_raw = sql_match.group(1).strip() # Store raw SQL
             logger.info(f"--- SQL Before Wrapping ---\n{extracted_sql_raw}") # Log raw SQL
             # --- ADDED: Wrap SQL comments ---
             extracted_sql = wrap_sql_comments(extracted_sql_raw, width=56)
             logger.info(f"--- SQL After Wrapping ---\n{extracted_sql}") # Log wrapped SQL
             # --- END WRAP ---
             if not extracted_sql.upper().startswith(("SELECT", "WITH")):
                  logger.error(f"LLM generated non-SELECT SQL: {extracted_sql}")
                  extracted_sql = "" # Invalidate if not SELECT/WITH
        else:
             logger.warning(f"Could not extract SQL using ```sql marker from LLM response: {llm_response_text}")

        # Extract Explanation
        explanation_match = re.search(r"```explanation\s*([\s\S]*?)\s*```", llm_response_text, re.IGNORECASE | re.DOTALL)
        extracted_explanation = ""
        if explanation_match:
            raw_explanation = explanation_match.group(1).strip()
            # --- ADDED: Format explanation --- 
            extracted_explanation = format_explanation_as_bullets(raw_explanation)
        else:
            logger.warning(f"Could not extract explanation using ```explanation marker from LLM response: {llm_response_text}")
            # Fallback logic
            fallback_explanation = ""
            if sql_match and sql_match.end() < len(llm_response_text):
                 remaining_text = llm_response_text[sql_match.end():].strip()
                 if remaining_text: fallback_explanation = remaining_text
                 else: fallback_explanation = "(AI did not provide a separate explanation.)"
            else:
                 fallback_explanation = llm_response_text.strip() if not sql_match else "(AI did not provide a formatted explanation.)"
            # Format fallback too
            extracted_explanation = format_explanation_as_bullets(fallback_explanation)

        # Return error if SQL is missing after extraction attempts
        if not extracted_sql:
             # Include explanation even if SQL fails, might be useful
             return jsonify({"error": "Failed to generate valid SQL query.", "sql": None, "explanation": extracted_explanation or None}), 500

        return jsonify({
            "sql": extracted_sql,
            "explanation": extracted_explanation,
            "error": None
        })

# --- Database Query Execution Endpoint ---
@app.route('/query', methods=['POST'])
def handle_db_query():
    """Handles SQL query execution requests from the frontend."""
    db_conn = None
    cursor = None
    sql_query = ""
    try:
        data = request.get_json()
        sql_query = data.get('sql', '').strip()
        if not sql_query: return jsonify({"error": "No query."}), 400
        # Basic safety check - allow SELECT and WITH (for CTEs)
        if not sql_query.upper().startswith(("SELECT", "WITH")):
            logger.warning(f"Blocking non-SELECT/WITH query: {sql_query[:100]}...")
            return jsonify({"error": "Only SELECT queries allowed."}), 400

        logger.info(f"Executing: {sql_query[:200]}...")
        conn_str = get_connection_string(); db_conn = psycopg2.connect(conn_str); cursor = db_conn.cursor()
        cursor.execute(sql_query); logger.info("Query executed.")
        results = {"columns": [], "data": [], "error": None, "message": "Query executed successfully."}

        if cursor.description:
            results["columns"] = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results["data"] = rows
            results["message"] = f"Success. Fetched {len(rows)} rows."
            logger.info(f"Fetched {len(rows)} rows.")
        else: # For non-SELECT commands (though blocked above) or commands without results
            rowcount = cursor.rowcount
            results["message"] = f"Command successful."
            if rowcount >= 0:
                 results["message"] += f" {rowcount} rows affected."
                 logger.info(f"Cmd rowcount: {rowcount}")

        # Use json.dumps with custom serializer to handle dates/decimals
        response_json = json.dumps(results, default=json_serial)
        return response_json, 200, {'ContentType':'application/json'}

    except psycopg2.Error as db_err:
        logger.error(f"DB Error: {db_err}\nQuery: {sql_query}")
        if db_conn: db_conn.rollback(); logger.info("DB rollback.")
        error_detail = str(db_err).split('\n')[0] # Concise error
        return jsonify({"error": f"DB Error: {error_detail}", "columns": [], "data": []}), 400 # Return 400 for bad query
    except Exception as e:
        logger.error(f"Unexpected query error: {e}\nQuery: {sql_query}", exc_info=True)
        if db_conn: db_conn.rollback(); logger.info("DB rollback.")
        return jsonify({"error": f"Unexpected server error: {str(e)}", "columns": [], "data": []}), 500 # Return 500 for server error
    finally:
        # Ensure resources are closed
        if cursor:
            cursor.close()
            logger.debug("Cursor closed.")
        if db_conn:
            db_conn.close()
            logger.debug("DB connection closed.")

# --- Run the App ---
if __name__ == '__main__':
    try:
        # Import Flask settings from config.py within this block
        from config import FLASK_HOST, FLASK_PORT, FLASK_DEBUG

        # Perform checks before starting server
        if not gemini_api_key:
             logger.critical("FATAL: Gemini API key not loaded. LLM features will fail.")
             sys.exit("Exiting: API key not loaded.")
        if not llm_model:
             logger.critical("FATAL: LLM model initialization failed. LLM features will fail.")
             sys.exit("Exiting: LLM model initialization failed.")
        if not SCHEMA_INFO:
             logger.warning("Schema information missing. LLM queries may be inaccurate.")

        logger.info(f"Starting Flask server on {FLASK_HOST}:{FLASK_PORT} (Debug: {FLASK_DEBUG})...")
        # Use threaded=False if using simple asyncio wrapper, or remove if not needed
        app.run(debug=FLASK_DEBUG, host=FLASK_HOST, port=FLASK_PORT)

    except ImportError:
         logger.error("Could not import Flask settings from config.py. Using defaults.")
         # Add checks here too before starting with defaults
         if not gemini_api_key: sys.exit("Exiting: API key not loaded.")
         if not llm_model: sys.exit("Exiting: LLM model init failed.")
         app.run(debug=True, host='127.0.0.1', port=5000) # Fallback defaults
    except Exception as e:
         logger.critical(f"Failed to start Flask app: {e}", exc_info=True)
         sys.exit("Exiting due to startup error.")

