# run_workflow
import subprocess
import sys
import os
import logging
from pathlib import Path

# Import config only to potentially get SCRIPT_DIR if needed,
# but better to define SCRIPT_DIR here based on __file__
# from config import SCRIPT_DIR # Not strictly needed if defined below

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).resolve().parent

def run_script(script_name: str) -> bool:
    """Runs a Python script using the same interpreter and checks for errors."""
    script_path = SCRIPT_DIR / script_name
    logger.info(f"--- Running {script_name} ---")
    try:
        # Ensure using the correct Python executable
        python_executable = sys.executable
        # Use utf-8 encoding for output capture
        result = subprocess.run(
            [python_executable, str(script_path)],
            check=True,                 # Raise CalledProcessError on non-zero exit code
            capture_output=True,
            text=True,                  # Decode stdout/stderr as text
            encoding='utf-8',           # Specify encoding for decoding
            errors='replace'            # Handle potential decoding errors gracefully
        )

        # Log stdout and stderr separately for clarity
        if result.stdout:
            logger.info(f"--- {script_name} STDOUT ---\n{result.stdout.strip()}")
        if result.stderr:
            # Use warning level for stderr as it might contain warnings or errors
            logger.warning(f"--- {script_name} STDERR ---\n{result.stderr.strip()}")

        logger.info(f"--- Finished {script_name} successfully ---")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"!!! ERROR running {script_name} !!!")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.error(f"--- {script_name} STDOUT ---\n{e.stdout.strip()}")
        if e.stderr:
            logger.error(f"--- {script_name} STDERR ---\n{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"!!! ERROR: Script not found at {script_path} !!!")
        return False
    except Exception as e:
        logger.exception(f"!!! An unexpected error occurred while running {script_name}: {e} !!!")
        return False

if __name__ == "__main__":
    logger.info("--- Starting Springsteen Data Workflow ---")

    # Step 1: Normalize the data
    # Assuming normalize_data.py exists in the same directory
    if not run_script("normalize_data.py"):
        logger.error("Workflow stopped due to error during data normalization.")
        sys.exit(1)

    # Step 2: Populate the database with normalized data
    # Assuming populate_database.py exists in the same directory
    # Pass command-line arguments through if needed (e.g., --clear)
    # For simplicity here, we run it without extra args, but you could add logic
    # to pass sys.argv[1:] to the populate_database call if necessary.
    if not run_script("populate_database.py"):
        logger.error("Workflow stopped due to error during database population.")
        sys.exit(1)

    logger.info("--- Springsteen Data Workflow Completed Successfully ---")