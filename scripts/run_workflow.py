import subprocess
import sys
import os
import logging
from pathlib import Path
import pandas as pd # Added for reading CSVs for summary

# Import config for paths and filenames
try:
    from config import OUTPUT_PATH, SONGS_OUTPUT_FILENAME, SHOWS_OUTPUT_FILENAME, SETLISTS_OUTPUT_FILENAME
except ImportError:
    logging.warning("Could not import from config. Attempting relative path for OUTPUT_PATH.")
    SCRIPT_DIR_FOR_PATH = Path(__file__).resolve().parent
    BASE_PATH_FOR_PATH = SCRIPT_DIR_FOR_PATH.parent
    OUTPUT_PATH = BASE_PATH_FOR_PATH / "3 - Schema Creation"
    SONGS_OUTPUT_FILENAME = "songs.csv"
    SHOWS_OUTPUT_FILENAME = "shows.csv"
    SETLISTS_OUTPUT_FILENAME = "setlists.csv"


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).resolve().parent

def run_script(script_name: str, args: list = None) -> bool:
    """Runs a Python script using the same interpreter and checks for errors."""
    script_path = SCRIPT_DIR / script_name
    # Ensure args is a list if None
    args = args or []
    logger.info(f"--- Running {script_name} {' '.join(args)} ---")
    try:
        python_executable = sys.executable
        command = [python_executable, str(script_path)]
        # Extend command with arguments if provided
        if args:
            command.extend(args)

        result = subprocess.run(
            command,
            check=True, # Raise CalledProcessError on non-zero exit code
            capture_output=True,
            text=True, # Decode output as text
            encoding='utf-8',
            errors='replace' # Replace undecodable characters
        )

        if result.stdout:
            logger.info(f"--- {script_name} STDOUT ---\n{result.stdout.strip()}")
        if result.stderr:
            logger.warning(f"--- {script_name} STDERR ---\n{result.stderr.strip()}")

        logger.info(f"--- Finished {script_name} successfully ---")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"!!! ERROR running {script_name} !!!")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout: logger.error(f"--- {script_name} STDOUT (error) ---\n{e.stdout.strip()}")
        if e.stderr: logger.error(f"--- {script_name} STDERR (error) ---\n{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"!!! ERROR: Script not found at {script_path} !!!")
        return False
    except Exception as e:
        logger.exception(f"!!! An unexpected error occurred while running {script_name}: {e} !!!")
        return False

if __name__ == "__main__":
    logger.info("--- Starting Springsteen Data Workflow ---")

    # --- REMOVED check for --clear in sys.argv ---

    # Step 1: Normalize the data
    # Run normalize_data.py without any arguments
    if not run_script("normalize_data.py"):
        logger.error("Workflow stopped due to error during data normalization.")
        sys.exit(1)

    # Step 2: Populate the database
    # --- MODIFIED: Always pass '--clear' argument ---
    logger.info("Running database population with automatic table clearing...")
    if not run_script("populate_database.py", args=["--clear"]):
    # --- END MODIFIED ---
        logger.error("Workflow stopped due to error during database population.")
        sys.exit(1)

    # Final Summary Counts (Unchanged)
    logger.info("--- Springsteen Data Workflow Completed Successfully ---")
    logger.info("--- Final Output Counts ---")
    try:
        songs_file = OUTPUT_PATH / SONGS_OUTPUT_FILENAME
        shows_file = OUTPUT_PATH / SHOWS_OUTPUT_FILENAME
        setlists_file = OUTPUT_PATH / SETLISTS_OUTPUT_FILENAME

        try:
            songs_df = pd.read_csv(songs_file)
            logger.info(f"- Songs ({SONGS_OUTPUT_FILENAME}): {len(songs_df)} entries")
        except FileNotFoundError: logger.error(f"- Error: Could not find {songs_file}")
        except Exception as e: logger.error(f"- Error reading {songs_file}: {e}")

        try:
            shows_df = pd.read_csv(shows_file)
            logger.info(f"- Shows ({SHOWS_OUTPUT_FILENAME}): {len(shows_df)} entries")
        except FileNotFoundError: logger.error(f"- Error: Could not find {shows_file}")
        except Exception as e: logger.error(f"- Error reading {shows_file}: {e}")

        try:
            setlists_df = pd.read_csv(setlists_file)
            logger.info(f"- Setlist Entries ({SETLISTS_OUTPUT_FILENAME}): {len(setlists_df)} entries")
        except FileNotFoundError: logger.error(f"- Error: Could not find {setlists_file}")
        except Exception as e: logger.error(f"- Error reading {setlists_file}: {e}")

    except NameError:
         logger.error("Could not determine output paths/filenames (config import likely failed). Cannot report counts.")
    except Exception as e:
        logger.error(f"An error occurred while trying to read output files for counts: {e}")
    # --- END: Final Summary Counts ---

