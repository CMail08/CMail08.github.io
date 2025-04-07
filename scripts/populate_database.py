# populate_database.py
# Populates the database using normalized CSV files.
# Automatically clears tables before import. REMOVED --clear flag and prompt.

import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
import os
import io
import csv
import sys
import logging
# import argparse # REMOVED argparse import
from typing import Tuple, List, Optional

# Import database configuration
try:
    from database_config import get_connection_string, get_postgres_connection_string, DB_NAME
except ImportError:
    print("ERROR: database_config.py not found or missing required elements.")
    sys.exit(1)

# Import path configuration
try:
    from config import OUTPUT_PATH, SCHEMA_SQL_FILE, SHOWS_OUTPUT_FILENAME, SONGS_OUTPUT_FILENAME, SETLISTS_OUTPUT_FILENAME
except ImportError:
     # Fallback if config import fails (e.g., script moved)
     print("ERROR: config.py not found or missing required elements (OUTPUT_PATH, SCHEMA_SQL_FILE, etc.). Attempting relative paths.")
     SCRIPT_DIR_FOR_PATH = Path(__file__).resolve().parent
     BASE_PATH_FOR_PATH = SCRIPT_DIR_FOR_PATH.parent
     OUTPUT_PATH = BASE_PATH_FOR_PATH / "3 - Schema Creation"
     SCHEMA_SQL_FILE = SCRIPT_DIR_FOR_PATH / "schema.sql"
     SHOWS_OUTPUT_FILENAME = "shows.csv"
     SONGS_OUTPUT_FILENAME = "songs.csv"
     SETLISTS_OUTPUT_FILENAME = "setlists.csv"


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Database Functions (create_db_if_not_exists, apply_schema, bulk_import_via_copy, update_statistics) ---
# (These functions remain unchanged from artifact populate_database_new_schema)
def create_db_if_not_exists():
    """Connects to the default 'postgres' database and creates the target database if it doesn't exist. Exits on failure."""
    conn = None; cursor = None
    try:
        logger.info(f"Connecting to 'postgres' database to check for '{DB_NAME}'...")
        pg_conn_str = get_postgres_connection_string()
        conn = psycopg2.connect(pg_conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cursor.fetchone()
        if not exists:
            logger.info(f"Database '{DB_NAME}' does not exist. Creating...")
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            logger.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logger.info(f"Database '{DB_NAME}' already exists.")
    except psycopg2.Error as e: logger.error(f"DB connection/creation error: {e}"); logger.critical("Cannot proceed. Exiting."); sys.exit(1)
    except Exception as e: logger.exception(f"Unexpected DB check/creation error: {e}"); logger.critical("Cannot proceed. Exiting."); sys.exit(1)
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        logger.debug("Closed connection to 'postgres' database.")

def apply_schema(conn: psycopg2.extensions.connection) -> bool:
    """Reads and executes the schema.sql file. Returns True on success, False on failure."""
    cursor = None
    try:
        schema_file_to_use = SCHEMA_SQL_FILE
        if not SCHEMA_SQL_FILE.exists():
             alt_schema_path = Path(__file__).resolve().parent / SCHEMA_SQL_FILE.name
             if not alt_schema_path.exists(): logger.error(f"Schema file not found at {SCHEMA_SQL_FILE} or {alt_schema_path}"); return False
             schema_file_to_use = alt_schema_path
             logger.warning(f"Using schema file found at: {schema_file_to_use}")
        logger.info(f"Applying schema from {schema_file_to_use.name}...")
        cursor = conn.cursor()
        with open(schema_file_to_use, 'r', encoding='utf-8') as f: sql_script = f.read(); cursor.execute(sql_script)
        conn.commit(); logger.info("Schema applied successfully."); return True
    except psycopg2.Error as e: logger.error(f"Schema error: {e} (State: {e.pgcode}, Details: {e.pgerror})"); conn.rollback(); return False
    except Exception as e: logger.exception(f"Unexpected schema error: {e}"); conn.rollback(); return False
    finally:
        if cursor: cursor.close()

def bulk_import_via_copy(conn: psycopg2.extensions.connection, df: pd.DataFrame, table_name: str, columns: Tuple[str, ...]) -> bool:
    """Imports data using COPY FROM STDIN. Returns True on success, False on failure."""
    if df.empty: logger.warning(f"DataFrame for table '{table_name}' empty. Skipping."); return True
    cursor = None; buffer = io.StringIO()
    try:
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols: logger.error(f"DF for '{table_name}' missing columns for COPY: {missing_cols}. Available: {df.columns.tolist()}"); return False
        df_copy = df[list(columns)].copy(); df_copy.fillna('', inplace=True)
        df_copy.to_csv(buffer, index=False, header=False, sep=',', quoting=csv.QUOTE_MINIMAL, na_rep='', quotechar='"', escapechar='\\')
        buffer.seek(0)
        logger.info(f"Bulk importing data into '{table_name}'...")
        cursor = conn.cursor()
        copy_sql = f"""COPY {table_name} ({", ".join(columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', NULL '', ESCAPE '\\')"""
        cursor.copy_expert(sql=copy_sql, file=buffer); conn.commit()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}"); final_count = cursor.fetchone()[0]
        logger.info(f"Import successful. Final row count for '{table_name}': {final_count}."); return True
    except psycopg2.Error as e: logger.error(f"COPY error for '{table_name}': {e}"); conn.rollback(); return False
    except KeyError as e: logger.error(f"Missing column during COPY prep for '{table_name}': {e}"); conn.rollback(); return False
    except Exception as e: logger.exception(f"Unexpected COPY error for '{table_name}': {e}"); conn.rollback(); return False
    finally:
        if cursor: cursor.close()
        buffer.close()

def update_statistics(conn: psycopg2.extensions.connection):
    """Calls stored functions to update song and show statistics. Exits on failure."""
    cursor = None
    try:
        logger.info("Updating statistics using stored functions...")
        cursor = conn.cursor()
        logger.info("Calling update_song_play_counts()..."); cursor.execute("SELECT update_song_play_counts();")
        notices = conn.notices; conn.notices.clear(); # Process notices if needed
        logger.info("Calling update_song_rarity_levels()..."); cursor.execute("SELECT update_song_rarity_levels();")
        notices = conn.notices; conn.notices.clear(); # Process notices if needed
        logger.info("Calling update_show_song_counts()..."); cursor.execute("SELECT update_show_song_counts();")
        notices = conn.notices; conn.notices.clear(); # Process notices if needed
        conn.commit(); logger.info("Statistics updated successfully.")
    except psycopg2.Error as e: logger.error(f"Stats update error: {e} (State: {e.pgcode})"); conn.rollback(); logger.critical("Failed. Exiting."); sys.exit(1)
    except Exception as e: logger.exception(f"Unexpected stats update error: {e}"); conn.rollback(); logger.critical("Failed. Exiting."); sys.exit(1)
    finally:
        if cursor: cursor.close()

# --- Main Execution ---
# MODIFIED: Removed clear_tables argument, clearing is now automatic
def populate_database():
    """Main function to create/verify DB, apply schema, clear tables, and populate data."""
    logger.info("--- Starting Database Population (Auto Clear Enabled) ---") # Updated log
    create_db_if_not_exists()
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        logger.info(f"Connecting to target database '{DB_NAME}'...")
        conn_str = get_connection_string()
        conn = psycopg2.connect(conn_str)
        logger.info("Connected successfully!")

        if not apply_schema(conn):
            logger.critical("Failed to apply database schema. Exiting."); sys.exit(1)

        logger.info(f"Reading normalized CSV files from {OUTPUT_PATH}...")
        try:
            shows_file = OUTPUT_PATH / SHOWS_OUTPUT_FILENAME
            songs_file = OUTPUT_PATH / SONGS_OUTPUT_FILENAME
            setlists_file = OUTPUT_PATH / SETLISTS_OUTPUT_FILENAME
            required_files = [shows_file, songs_file, setlists_file]
            if not all(f.exists() for f in required_files):
                 missing = [f.name for f in required_files if not f.exists()]
                 raise FileNotFoundError(f"Required CSV file(s) not found in {OUTPUT_PATH}: {', '.join(missing)}")
            shows_df = pd.read_csv(shows_file, encoding='utf-8-sig', keep_default_na=False)
            songs_df = pd.read_csv(songs_file, encoding='utf-8-sig', keep_default_na=False)
            setlists_df = pd.read_csv(setlists_file, encoding='utf-8-sig', keep_default_na=False)
            logger.info(f"Loaded {len(shows_df)} shows, {len(songs_df)} songs, {len(setlists_df)} setlist entries.")

            logger.info("Preparing DataFrames for database import...")
            # Prep logic unchanged...
            songs_df['song_id'] = songs_df['song_id'].astype(int)
            songs_df['is_outtake'] = songs_df['is_outtake'].astype(bool)
            for col in ['title', 'album']: songs_df[col] = songs_df[col].astype(str)
            shows_copy_cols_list = ['show_id', 'date', 'tour', 'venue', 'city', 'state_name', 'state_code', 'country_name', 'country_code', 'show_notes']
            shows_df['show_id'] = shows_df['show_id'].astype(int)
            shows_df['date'] = pd.to_datetime(shows_df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            for col in shows_copy_cols_list:
                 if col == 'show_id' or col == 'date': continue
                 if col in shows_df.columns: shows_df[col] = shows_df[col].astype(str)
                 else: logger.warning(f"Column '{col}' missing in shows_df, adding empty."); shows_df[col] = ""
            setlists_df['setlist_entry_id'] = setlists_df['setlist_entry_id'].astype(int)
            setlists_df['show_id'] = setlists_df['show_id'].astype(int)
            setlists_df['song_id'] = setlists_df['song_id'].astype(int)
            setlists_df['position'] = pd.to_numeric(setlists_df['position'], errors='coerce').fillna(0).astype(int)
            setlists_df['notes'] = setlists_df['notes'].astype(str)
            logger.info("DataFrames prepared.")

        except FileNotFoundError as e: logger.error(e); logger.critical("Cannot proceed. Exiting."); sys.exit(1)
        except Exception as e: logger.exception(f"Failed to read/process CSV: {e}"); logger.critical("Cannot proceed. Exiting."); sys.exit(1)

        # --- MODIFIED: Always clear tables ---
        logger.warning("Automatically clearing existing data from tables...")
        cursor = None
        try:
            cursor = conn.cursor()
            logger.info(" Clearing setlists...")
            cursor.execute("TRUNCATE TABLE setlists RESTART IDENTITY CASCADE;")
            logger.info(" Clearing shows...")
            cursor.execute("TRUNCATE TABLE shows RESTART IDENTITY CASCADE;")
            logger.info(" Clearing songs...")
            cursor.execute("TRUNCATE TABLE songs RESTART IDENTITY CASCADE;")
            conn.commit(); logger.info("Tables cleared.")
        except psycopg2.Error as e: logger.error(f"Error clearing tables: {e}"); conn.rollback(); logger.critical("Failed to clear tables. Exiting."); sys.exit(1)
        finally:
            if cursor: cursor.close()
        # --- END MODIFIED ---

        # Define columns for COPY
        songs_copy_cols = ('song_id', 'title', 'album', 'is_outtake')
        shows_copy_cols = ('show_id', 'date', 'tour', 'venue', 'city',
                           'state_name', 'state_code', 'country_name', 'country_code',
                           'show_notes')
        setlists_copy_cols = ('setlist_entry_id', 'show_id', 'song_id', 'position', 'notes')

        logger.info("Starting data import...")
        if not bulk_import_via_copy(conn, songs_df, 'songs', songs_copy_cols): sys.exit(1)
        if not bulk_import_via_copy(conn, shows_df, 'shows', shows_copy_cols): sys.exit(1)
        if not bulk_import_via_copy(conn, setlists_df, 'setlists', setlists_copy_cols): sys.exit(1)
        logger.info("Data import completed successfully.")

        # Update statistics
        update_statistics(conn)

        logger.info("--- Database Population Complete ---")

    except psycopg2.Error as e: logger.error(f"Database connection error: {e}"); sys.exit(1)
    except Exception as e: logger.exception(f"Unexpected error: {e}"); sys.exit(1)
    finally:
        if conn: conn.close(); logger.info("Database connection closed.")

# --- Main execution block ---
if __name__ == "__main__":
    # --- REMOVED Argument Parsing and Confirmation Prompt ---
    # No longer need to check for --clear or ask for confirmation
    # ---
    # Always run with clearing enabled (handled inside the function)
    populate_database()
