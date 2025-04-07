# populate_data
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
import os
import io       # Needed for StringIO buffer
import csv      # Needed for CSV quoting options
import sys      # To check for command line arguments
import logging
import argparse # For command-line argument parsing
from typing import Tuple, List, Optional

# Import database configuration (ensure this file exists and is secure)
# It should define DB_NAME, get_connection_string(), get_postgres_connection_string()
try:
    from database_config import get_connection_string, get_postgres_connection_string, DB_NAME
except ImportError:
    print("ERROR: database_config.py not found or missing required elements.")
    print("Please create database_config.py with DB_NAME, get_connection_string(), and get_postgres_connection_string().")
    sys.exit(1)

# Import path configuration
from config import SCHEMA_CREATION_PATH, SCHEMA_SQL_FILE, SHOWS_OUTPUT_FILENAME, SONGS_OUTPUT_FILENAME, SETLISTS_OUTPUT_FILENAME

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Database Functions ---

def create_db_if_not_exists():
    """Connects to the default 'postgres' database and creates the target database if it doesn't exist."""
    conn = None
    cursor = None
    try:
        # Connect to the default 'postgres' database
        logger.info(f"Connecting to 'postgres' database to check for '{DB_NAME}'...")
        # IMPORTANT: Ensure get_postgres_connection_string() retrieves credentials securely (e.g., env vars)
        pg_conn_str = get_postgres_connection_string()
        conn = psycopg2.connect(pg_conn_str)
        conn.autocommit = True  # Use autocommit for database creation/check
        cursor = conn.cursor()

        # Check if the target database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"Database '{DB_NAME}' does not exist. Creating...")
            # IMPORTANT: Ensure DB_NAME is validated or comes from a trusted source if dynamic.
            # Using f-string carefully here as DB_NAME comes from config.
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            logger.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logger.info(f"Database '{DB_NAME}' already exists.")

    except psycopg2.Error as e:
        logger.error(f"Database connection or creation error: {e}")
        sys.exit(1) # Exit if database connection/creation fails
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Closed connection to 'postgres' database.")


def apply_schema(conn: psycopg2.extensions.connection):
    """Reads and executes the schema.sql file."""
    cursor = None
    try:
        if not SCHEMA_SQL_FILE.exists():
            logger.error(f"Schema file not found at {SCHEMA_SQL_FILE}")
            return False

        logger.info(f"Applying schema from {SCHEMA_SQL_FILE.name}...")
        cursor = conn.cursor()
        with open(SCHEMA_SQL_FILE, 'r', encoding='utf-8') as f:
            sql_script = f.read()
            cursor.execute(sql_script)
        conn.commit() # Commit schema changes
        logger.info("Schema applied successfully.")
        return True

    except psycopg2.Error as e:
        logger.error(f"Error applying schema: {e}")
        conn.rollback() # Rollback in case of error during schema application
        return False
    finally:
        if cursor:
            cursor.close()


def bulk_import_via_copy(conn: psycopg2.extensions.connection, df: pd.DataFrame, table_name: str, columns: Tuple[str, ...]) -> int:
    """
    Imports data from a pandas DataFrame into a PostgreSQL table using COPY FROM STDIN.

    Args:
        conn: Active psycopg2 database connection.
        df (pd.DataFrame): DataFrame containing the data to import.
        table_name (str): Name of the target database table.
        columns (tuple): Tuple of column names in the order they appear in the DataFrame
                         and match the target table columns for COPY.

    Returns:
        Number of rows reported as imported by counting after copy (or 0 on error).
    """
    if df.empty:
        logger.warning(f"DataFrame for table '{table_name}' is empty. Skipping import.")
        return 0

    cursor = None
    buffer = io.StringIO() # Use in-memory string buffer

    # Write DataFrame to buffer as CSV, quoting minimal fields and handling None as empty string
    # na_rep='' ensures missing values become empty strings, matching NULL '' in COPY
    df.to_csv(buffer, index=False, header=False, sep=',',
              quoting=csv.QUOTE_MINIMAL, na_rep='', quotechar='"')

    buffer.seek(0) # Rewind buffer to the beginning

    try:
        logger.info(f"Bulk importing data into '{table_name}'...")
        cursor = conn.cursor()

        # Construct the COPY command
        # Using CSV format and specifying columns explicitly
        copy_sql = f"""
            COPY {table_name} ({", ".join(columns)})
            FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', NULL '')
        """

        # Execute the COPY command
        cursor.copy_expert(sql=copy_sql, file=buffer)

        # Get a reliable count *after* the copy operation
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        current_count = cursor.fetchone()[0] # Get count *before* commit for logging change
        logger.info(f"COPY command executed for '{table_name}'. Verifying row count...")

        # Commit after successful copy
        # NOTE: For full atomicity, commit should happen *outside* this function
        # after ALL tables are loaded. Keeping it here maintains original behavior.
        conn.commit()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]

        # Calculate rows added in this specific operation (approximate)
        # This isn't perfect if rows existed before, but gives an indication
        # rows_added = final_count - current_count_before_commit (if you track count before COPY)
        logger.info(f"Successfully imported data. Final row count for '{table_name}': {final_count}.")
        # Return the final count as a measure of success/total rows
        return final_count

    except psycopg2.Error as e:
        logger.error(f"Error during bulk import into '{table_name}': {e}")
        conn.rollback() # Rollback on error for this specific table import
        return 0 # Indicate failure/zero rows imported
    except Exception as e:
        logger.exception(f"Unexpected error during bulk import into '{table_name}': {e}")
        conn.rollback()
        return 0
    finally:
        if cursor:
            cursor.close()
        buffer.close() # Close the string buffer


def update_statistics(conn: psycopg2.extensions.connection):
    """Calls stored functions to update song statistics."""
    cursor = None
    try:
        logger.info("Updating song statistics using stored functions...")
        cursor = conn.cursor()

        logger.info("Calling update_song_play_counts()...")
        cursor.execute("SELECT update_song_play_counts();")
        # Capture and log notices/messages from the function
        for notice in conn.notices:
            logger.info(f"DB Notice (play counts): {notice.strip()}")
        conn.notices.clear() # Clear notices for the next function call

        logger.info("Calling update_song_rarity_levels()...")
        cursor.execute("SELECT update_song_rarity_levels();")
        for notice in conn.notices:
             logger.info(f"DB Notice (rarity levels): {notice.strip()}")
        conn.notices.clear()

        # Commit statistic updates
        # NOTE: For full atomicity, commit should happen *outside* this function
        # after ALL tables are loaded AND stats updated. Keeping it here maintains original behavior.
        conn.commit()
        logger.info("Song statistics updated successfully.")

    except psycopg2.Error as e:
        logger.error(f"Error updating statistics: {e}")
        conn.rollback()
    except Exception as e:
        logger.exception(f"Unexpected error updating statistics: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()

# --- Main Execution ---
def populate_database(clear_tables: bool = False):
    """
    Main function to create/verify DB, apply schema, and populate data.

    Args:
        clear_tables (bool): If True, truncates tables before importing. Defaults to False.
    """
    logger.info("--- Starting Database Population ---")

    # 1. Ensure database exists
    create_db_if_not_exists()

    # 2. Connect to the target database
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        logger.info(f"Connecting to target database '{DB_NAME}'...")
        # IMPORTANT: Ensure get_connection_string() retrieves credentials securely
        conn_str = get_connection_string()
        conn = psycopg2.connect(conn_str)
        logger.info("Connected successfully!")

        # 3. Apply the schema from schema.sql
        if not apply_schema(conn):
            logger.error("Exiting due to schema application error.")
            sys.exit(1)

        # 4. Load data from normalized CSV files
        logger.info(f"Reading normalized CSV files from {SCHEMA_CREATION_PATH}...")
        try:
            shows_file = SCHEMA_CREATION_PATH / SHOWS_OUTPUT_FILENAME
            songs_file = SCHEMA_CREATION_PATH / SONGS_OUTPUT_FILENAME
            setlists_file = SCHEMA_CREATION_PATH / SETLISTS_OUTPUT_FILENAME

            # Check if files exist
            required_files = [shows_file, songs_file, setlists_file]
            if not all(f.exists() for f in required_files):
                 missing = [f.name for f in required_files if not f.exists()]
                 logger.error(f"ERROR: Required CSV file(s) not found: {', '.join(missing)}")
                 logger.error("Please ensure normalize_data.py has run successfully.")
                 sys.exit(1)

            shows_df = pd.read_csv(shows_file, encoding='utf-8-sig')
            songs_df = pd.read_csv(songs_file, encoding='utf-8-sig')
            setlists_df = pd.read_csv(setlists_file, encoding='utf-8-sig')

            logger.info(f"Loaded {len(shows_df)} shows, {len(songs_df)} songs, {len(setlists_df)} setlist entries.")

            # Data Cleaning/Preparation before COPY
            # Ensure correct types and handle NaNs -> None/Empty String for DB compatibility with COPY
            logger.info("Preparing DataFrames for database import...")
            shows_df['show_id'] = shows_df['show_id'].astype(int)
            # Ensure date is valid date object then format, handle potential errors robustly
            shows_df['date'] = pd.to_datetime(shows_df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            # Replace any remaining NaNs (incl. NaT from date conversion failure) with empty strings for COPY NULL ''
            shows_df = shows_df.fillna('')

            songs_df['song_id'] = songs_df['song_id'].astype(int)
            songs_df['is_outtake'] = songs_df['is_outtake'].astype(bool)
            # Replace NaNs in string columns (like album, title) with empty strings
            songs_df = songs_df.fillna('')

            setlists_df['setlist_entry_id'] = setlists_df['setlist_entry_id'].astype(int)
            setlists_df['show_id'] = setlists_df['show_id'].astype(int)
            setlists_df['song_id'] = setlists_df['song_id'].astype(int)
            # Ensure position is integer, default 0 if NaN or cannot convert
            setlists_df['position'] = pd.to_numeric(setlists_df['position'], errors='coerce').fillna(0).astype(int)
            # Replace NaNs in notes column
            setlists_df = setlists_df.fillna('')

            logger.info("DataFrames prepared.")

        except FileNotFoundError as e:
            logger.error(f"Cannot find input file during read: {e}")
            sys.exit(1)
        except Exception as e:
            logger.exception(f"Failed to read or process CSV files: {e}")
            sys.exit(1)

        # 5. Clear existing data (Conditional)
        if clear_tables:
            logger.warning("Clearing existing data from tables as requested...")
            cursor = None
            try:
                cursor = conn.cursor()
                # Clear in reverse order of dependencies
                logger.info(" Clearing setlists...")
                cursor.execute("TRUNCATE TABLE setlists RESTART IDENTITY CASCADE;")
                logger.info(" Clearing shows...")
                cursor.execute("TRUNCATE TABLE shows RESTART IDENTITY CASCADE;")
                logger.info(" Clearing songs...")
                cursor.execute("TRUNCATE TABLE songs RESTART IDENTITY CASCADE;") # Also resets calculated columns
                conn.commit()
                logger.info("Tables cleared.")
            except psycopg2.Error as e:
                logger.error(f"Error clearing tables: {e}")
                conn.rollback()
                sys.exit(1)
            finally:
                if cursor: cursor.close()

        # 6. Bulk import data using COPY FROM
        # Prepare DataFrames for COPY (select and order columns explicitly matching the COPY command)
        songs_copy_df = songs_df[['song_id', 'title', 'album', 'is_outtake']]
        shows_copy_df = shows_df[['show_id', 'date', 'tour', 'city', 'venue', 'region']]
        setlists_copy_df = setlists_df[['setlist_entry_id', 'show_id', 'song_id', 'position', 'notes']]

        # --- Import Data ---
        # Note on Transactions: Each bulk_import call currently commits internally.
        # For full atomicity (all or nothing), move commit() outside after all imports
        # and statistics update, and rollback on any error. See comments within functions.

        bulk_import_via_copy(conn, songs_copy_df, 'songs', ('song_id', 'title', 'album', 'is_outtake'))
        bulk_import_via_copy(conn, shows_copy_df, 'shows', ('show_id', 'date', 'tour', 'city', 'venue', 'region'))
        bulk_import_via_copy(conn, setlists_copy_df, 'setlists', ('setlist_entry_id', 'show_id', 'song_id', 'position', 'notes'))

        # 7. Update calculated statistics
        update_statistics(conn)

        logger.info("--- Database Population Complete ---")

    except psycopg2.Error as e:
        logger.error(f"Database connection or population error: {e}")
        if conn:
            conn.rollback() # Rollback any partial changes if error outside specific loads
    except Exception as e:
        logger.exception(f"An unexpected error occurred during population: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate the Springsteen database from normalized CSV files.")
    parser.add_argument(
        '--clear',
        action='store_true', # Sets clear_tables to True if flag is present
        help="Truncate (clear) tables before importing data. Use with caution!"
    )
    args = parser.parse_args()

    clear_tables_flag = args.clear

    if clear_tables_flag:
        confirm = input("WARNING: You specified --clear. This will delete all existing data in the tables. Are you sure? (yes/no): ").lower().strip()
        if confirm != 'yes':
            logger.info("Aborting. Tables will not be cleared.")
            sys.exit(0)

    # Run the population process
    populate_database(clear_tables=clear_tables_flag)