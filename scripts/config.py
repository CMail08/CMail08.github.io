# config.py
# Simplified configuration for sequence-based matching.

import re
from pathlib import Path

# --- Path Configuration ---
# Assumes this config.py is in the 'scripts' directory
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_PATH = SCRIPT_DIR.parent # Main project folder (e.g., Springsteen_Setlists)
INPUT_PATH = BASE_PATH / "2 - Data Sources" # Folder for raw Setlists.csv, Sessions.csv
OUTPUT_PATH = BASE_PATH / "3 - Schema Creation" # Folder for normalized songs.csv, shows.csv, setlists.csv
SCHEMA_SQL_FILE = SCRIPT_DIR / "schema.sql" # Location of the schema file relative to this script

# --- Input File Names ---
SONGS_SESSIONS_FILENAME = "Sessions, Outtakes, Songs.csv"
SETLISTS_FILENAME = "Setlists.csv" # Raw data input from Set_List_Finder.py

# --- Output File Names (Normalized Data) ---
SONGS_OUTPUT_FILENAME = "songs.csv"
SHOWS_OUTPUT_FILENAME = "shows.csv"
SETLISTS_OUTPUT_FILENAME = "setlists.csv"

# --- Output File Names (Data Collection) ---
# Output file for Setlist_Finder test mode
TEST_SETLISTS_OUTPUT_FILENAME = "Updated_Setlist_Finder_TEST.csv"

# --- Database Configuration ---
# Database connection details are defined in database_config.py

# --- Setlist.fm API Configuration (`Set_List_Finder.py`) ---
API_KEY_FILENAME = "API Key.txt" # Name of the API key file (expected in same dir as Set_List_Finder.py)
BRUCE_MBID = "70248960-cb53-4ea4-943a-edb18f7d336f" # MusicBrainz ID for Bruce Springsteen
SETLISTFM_BASE_URL = "https://api.setlist.fm/rest/1.0/search/setlists" # API Endpoint
REQUEST_TIMEOUT = 25 # Seconds before API request times out
MAX_RETRIES_PER_PAGE = 3 # Retries on transient API errors
MAX_CONCURRENT_REQUESTS = 1 # Max simultaneous API calls (1 for safety)
SUCCESS_DELAY = 1.0 # Seconds to pause after a successful API call (to respect rate limits)
STATUS_UPDATE_INTERVAL = 10 # Seconds between checking for console silence during fetch
SILENCE_THRESHOLD = 30 # Seconds of silence before printing status update

# --- Normalization Configuration (`normalize_data.py`) ---

# Minimal replacements applied BEFORE splitting titles into words for matching
# Aims to remove common disruptive punctuation/symbols
MINIMAL_REPLACEMENTS = {
    "(": " ", ")": " ", "[": " ", "]": " ", "{": " ", "}": " ", # Replace brackets with space
    "!": "", "?": "", ".": "", # Remove terminal/common punctuation
    "&": " and ", "+": " and ", # Standardize connectors
     "/": " ", "\\": " ", # Replace slashes with space
     " - ": " ", "-": " ", "_": " ", # Standardize separators
     ":": "", ";": "", "\"": "", # Remove other punctuation that might interfere
     # Keeping apostrophe (') and asterisk (*) within words for now
}

# Basic regex to collapse multiple whitespace characters into one
RE_WHITESPACE = re.compile(r'\s+')

# Specific settings for contextual logic in normalize_data.py
# Set of lowercase raw tour names expected for the Reunion Tour
REUNION_TOUR_RAW_NAMES = {"reunion tour", "reunion"}
# The exact title string from Sessions, Outtakes, Songs.csv for the specific BITUSA version played on Reunion tour
# User confirmed this title is correct based on their sessions data
REUNION_BITUSA_LOOKUP_TITLE = "Born In The USA (Acoustic)"

# --- Web Application Configuration (`app.py`) ---
# LLM Model to use (check Google AI docs for available model IDs)
LLM_MODEL_NAME = 'gemini-2.5-pro-exp-03-25' # Defaulting to Flash
# Experimental model ID (use if you have access and confirmed ID):
# LLM_MODEL_NAME = 'models/gemini-2.5-pro-exp-03-25'

# Flask App Settings
FLASK_HOST = '127.0.0.1' # Use '0.0.0.0' to make accessible on local network
FLASK_PORT = 5000
FLASK_DEBUG = True # Set to False for production

