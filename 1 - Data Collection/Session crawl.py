# Session_Crawler.py
# Applies MINIMAL normalization to song titles scraped from Brucebase.
# Writes output to the specified TEST file using relative paths.
# Corrected fetch_html_with_retry function.

import re
import time
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path # Use pathlib

# --- Configuration ---
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Use logger instead of print for most messages

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR.parent / "2 - Data Sources"
OUTPUT_FILENAME = "Test.csv"
OUTPUT_CSV = OUTPUT_DIR / OUTPUT_FILENAME

ALBUMS = [ # Shortened list for faster testing if needed
    "Greetings From Asbury Park NJ", "Wild Innocent & E Street Shuffle", "Born To Run",
    "Darkness On The Edge Of Town", "The River", "Nebraska", "Born In The USA",
    # "Tunnel Of Love", "Human Touch", "Lucky Town", "The Ghost Of Tom Joad",
    # "The Rising", "Devils and Dust", "The Seeger Sessions", "Magic",
    # "Working On A Dream", "Wrecking Ball", "High Hopes", "Western Stars",
    # "Letter To You", "Only The Strong Survive"
]

# Minimal Cleaning Rules
TITLE_REPLACEMENTS_MINIMAL = { "&": "and", "+": "and", "@": "at", "W/": "with ", "W /": "with ", "W/ ": "with ", "FEAT.": "featuring", "FEAT": "featuring", "FT.": "featuring", "FT": "featuring", " - ": " ", "-": " ", "_": " ", ".": "", ",": "", "!": "", "?": "", "(": "", ")": "", "[": "", "]": "", "{": "", "}": "", ":": "", ";": "", "\"": "", "/": " ", "\\": " ", }
RE_CLEAN_EDGES = re.compile(r"^[^\w'*]+|[^\w'\s*.]+$")
RE_WHITESPACE = re.compile(r'\s+')

# --- Helper Functions ---
def album_to_slug(name: str) -> str:
    """Convert album name to Brucebase URL slug"""
    if name == "The Seeger Sessions": return "the-seeger-sessions"
    lower = name.lower(); lower = re.sub(r"[^\w\s-]", "", lower); lower = re.sub(r"\s+", " ", lower).strip()
    slug = lower.replace(" ", "-"); return slug

# --- CORRECTED fetch_html_with_retry function ---
def fetch_html_with_retry(url: str) -> str:
    """Fetch HTML with basic retry logic"""
    for attempt in range(3):
        response = None # Initialize response to None for safety
        try:
            logger.debug(f"Attempt {attempt+1}/3 fetching {url}...") # Use debug for fetch attempt
            response = requests.get(url, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            # If we get here, the request was successful (status 2xx)
            logger.debug(f"Successfully fetched {url} on attempt {attempt+1}")
            return response.text # Return successful response text

        except requests.exceptions.Timeout:
             logger.warning(f"Timeout fetching {url}, retrying ({attempt+1}/3)...")
             # Let loop continue to retry after delay

        except requests.exceptions.HTTPError as http_err:
            # Handle specific HTTP errors if needed, otherwise log and retry
            logger.warning(f"HTTP error {http_err.response.status_code} fetching {url}, retrying ({attempt+1}/3)...")
            # Let loop continue to retry after delay

        except requests.exceptions.RequestException as e:
            # Catch other potential network errors
            logger.warning(f"Request error fetching {url}: {e}, retrying ({attempt+1}/3)...")
            # Let loop continue to retry after delay

        # Wait before retrying
        time.sleep(2)

    # If loop finishes, all retries failed - try one last time with longer timeout
    logger.warning(f"Initial retries failed for {url}. Trying one last time with longer timeout...")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status() # Check final attempt
        logger.info(f"Successfully fetched {url} on final attempt.")
        return response.text
    except requests.exceptions.RequestException as e:
        # Log final failure comprehensively
        status_code_str = 'N/A'
        if hasattr(e, 'response') and e.response is not None:
            status_code_str = str(e.response.status_code)
        logger.error(f"Failed fetch {url} after all retries. Final error: {e} (Status Code: {status_code_str})")
        return f"Failed: {e}" # Return error message
# --- END CORRECTED fetch_html_with_retry ---


# --- MINIMAL clean_song_title FUNCTION ---
def clean_song_title(title):
    """ Apply MINIMAL cleaning to song titles fetched from Brucebase. """
    if not title or not isinstance(title, str) or not title.strip(): return ""
    title_str = title.strip() # Preserve original case
    for old, new in TITLE_REPLACEMENTS_MINIMAL.items(): title_str = title_str.replace(old, new)
    # Remove specific leading/trailing patterns
    title_str = re.sub(r'^\d+\.\s*', '', title_str); title_str = re.sub(r'^\d+\.\d+\.\s*', '', title_str)
    title_str = re.sub(r'^\d+\)\s*', '', title_str); title_str = re.sub(r'^\#\s*', '', title_str)
    title_str = re.sub(r'\s*\[V\d+\]$', '', title_str, flags=re.IGNORECASE)
    title_str = re.sub(r'\s*\(V\d+\)$', '', title_str, flags=re.IGNORECASE)
    title_str = re.sub(r'\s*\(\#\d+\)$', '', title_str)
    title_str = re.sub(r'\s*-\s*Parts?\s*\d+\s*&\s*\d+$', '', title_str, flags=re.IGNORECASE)
    title_str = re.sub(r'\s*\((Fast|Slow)\s+Version\)$', '', title_str, flags=re.IGNORECASE)
    title_str = re.sub(r'\s*alternate\s+take\s*\d+$', '', title_str, flags=re.IGNORECASE)
    # Clean edges, collapse whitespace
    title_str = RE_CLEAN_EDGES.sub('', title_str); title_str = RE_WHITESPACE.sub(' ', title_str).strip()
    if not title_str: return ""
    return title_str
# --- END MINIMAL clean_song_title ---

# --- extract_songs uses the corrected logic from previous response ---
def extract_songs(html_content, album_name):
    """Extract songs from Brucebase HTML - CORRECTED EXTRACTION LOGIC"""
    soup = BeautifulSoup(html_content, "html.parser"); results = []; processed_in_section = set()
    released_span = soup.find('span', string="Released"); additional_span = soup.find('span', string="Additional Recordings")
    details_span = soup.find('span', string="Details")

    def extract_from_table(table, session_name, song_type):
        count = 0
        for row_idx, row in enumerate(table.find_all('tr')):
            cells = row.find_all('td');
            if not cells or row.find('th'): continue
            # More robust header check based on common labels in first cell
            first_cell_text = cells[0].get_text(strip=True).lower()
            if first_cell_text in ['track', 'song title', '#', 'date', '']: continue

            title_text = "";
            # Try second column first
            if len(cells) > 1: title_text = cells[1].get_text(strip=True)
            # If second was empty or looks like noise, try first
            if not title_text or title_text.isdigit() or re.match(r'^[A-Z0-9-]+$', title_text) or re.match(r'^\w+ \d{1,2}, \d{4}$', title_text):
                 if len(cells) > 0: title_text = cells[0].get_text(strip=True)
                 else: title_text = "" # Ensure reset

            song_title_cleaned = clean_song_title(title_text)
            entry_key = (song_title_cleaned, song_type)
            if song_title_cleaned and entry_key not in processed_in_section:
                logger.debug(f"Found {song_type}: '{song_title_cleaned}' (from '{title_text}')") # Debug log
                results.append({"session": session_name, "type": song_type, "song": song_title_cleaned})
                processed_in_section.add(entry_key); count += 1
            elif song_title_cleaned and entry_key in processed_in_section:
                logger.debug(f"Skipping duplicate in section: {entry_key}")
            # else: logger.debug(f"Discarded row {row_idx}: Raw='{title_text}', Cleaned='{song_title_cleaned}'") # Debug discarded

        return count

    album_tracks_found = 0
    if released_span:
        logger.debug(f"Scanning 'Released' section for {album_name}...")
        current = released_span.find_next_sibling()
        while current and (not additional_span or current.find_previous_sibling('span') == released_span) and (not details_span or current.find_previous_sibling('span') != details_span):
             if current.name == 'table': album_tracks_found += extract_from_table(current, album_name, "Album")
             if current == additional_span or current == details_span: break
             current = current.find_next_sibling()
        logger.debug(f"Found {album_tracks_found} album tracks for {album_name}.")

    outtakes_found = 0
    if additional_span:
        logger.debug(f"Scanning 'Additional Recordings' section for {album_name}...")
        current = additional_span.find_next_sibling()
        while current and (not details_span or current.find_previous_sibling('span') == additional_span):
             if current.name == 'table': outtakes_found += extract_from_table(current, album_name, "Outtake")
             if current == details_span: break
             current = current.find_next_sibling()
        logger.debug(f"Found {outtakes_found} potential outtake tracks for {album_name}.")

    if album_tracks_found == 0 and outtakes_found == 0:
        logger.warning(f"No songs found for {album_name}. Check Brucebase page structure or HTML content.")

    return results

# --- write_to_excel_csv remains the same ---
def write_to_excel_csv(all_results, output_file):
    """Write song data to a Microsoft Excel-compatible CSV file."""
    logger.info(f"Writing {len(all_results)} songs to CSV: {output_file}")
    if not all_results: logger.warning("No results to write."); return False
    df = pd.DataFrame(all_results)
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if not df.empty: df = df[['session', 'type', 'song']]
        else: df = pd.DataFrame(columns=['session', 'type', 'song'])
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Successfully wrote {len(all_results)} rows to {output_file}")
        return True
    except Exception as e: logger.error(f"Error writing CSV file {output_file}: {e}"); return False

# --- main function remains the same ---
def main():
    logger.info("Starting Springsteen album session data collection (Brucebase)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory set to: {OUTPUT_DIR}")
    logger.info(f"Output file set to: {OUTPUT_FILENAME}")

    all_results = []; seen_records = set()
    for album in ALBUMS:
        slug = album_to_slug(album)
        url = f"http://brucebase.wikidot.com/stats:{slug}-studio-sessions"
        logger.info(f"\nProcessing: {album} -> {url}")
        html_content = fetch_html_with_retry(url)
        if html_content.startswith("Failed"): logger.error(f"Failed to fetch {url}: {html_content}"); continue

        song_data = extract_songs(html_content, album)
        added_count = 0
        for item in song_data:
            record_key = (item["session"], item["type"], item["song"])
            if record_key not in seen_records:
                all_results.append(item); seen_records.add(record_key); added_count += 1
        logger.info(f"Added {added_count} unique entries for {album}.")

    logger.info(f"\nTotal unique entries found across all albums: {len(all_results)}")
    write_success = write_to_excel_csv(all_results, OUTPUT_CSV)

    # Corrected Summary Logic
    if write_success and OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size > 0:
        try:
            df_final = pd.read_csv(OUTPUT_CSV)
            total_written = len(df_final)
            if not df_final.empty and 'type' in df_final.columns:
                album_count = len(df_final[df_final['type'] == "Album"])
                outtake_count = len(df_final[df_final['type'] == "Outtake"])
            else: album_count = 0; outtake_count = 0; logger.warning("Output CSV missing 'type' column or is empty.")
            logger.info(f"\nFinal Summary (after writing):")
            logger.info(f"- Total unique rows written: {total_written}")
            logger.info(f"- Album tracks listed: {album_count}")
            logger.info(f"- Outtake tracks listed: {outtake_count}")
        except Exception as read_err: logger.error(f"\nError reading back CSV for summary: {read_err}")
    elif not all_results: logger.info("\nFinal Summary: No songs found or written.")
    else: logger.warning("\nWarning: Output CSV file may not have been written correctly or is empty.")

    logger.info(f"\nDone! Springsteen songs data saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()