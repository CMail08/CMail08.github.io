

# Set_List_Finder.py
# Uses configuration from config.py for API settings.

import time
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys
import argparse
import asyncio
import aiohttp
from typing import Optional, Dict, List, Any

# --- Import Configuration ---
try:
    from config import (
        INPUT_PATH, # Base path for data sources where Setlists.csv will be saved
        SETLISTS_FILENAME, # Main output filename
        TEST_SETLISTS_OUTPUT_FILENAME, # Test output filename
        API_KEY_FILENAME, # Name of the API key file
        BRUCE_MBID,
        SETLISTFM_BASE_URL,
        REQUEST_TIMEOUT,
        MAX_RETRIES_PER_PAGE,
        MAX_CONCURRENT_REQUESTS,
        SUCCESS_DELAY,
        STATUS_UPDATE_INTERVAL,
        SILENCE_THRESHOLD
    )
except ImportError:
    logging.basicConfig(level=logging.CRITICAL)
    logging.critical("ERROR: config.py not found or missing required variables.")
    logging.critical("Please ensure config.py is accessible and contains necessary settings.")
    sys.exit("Exiting due to missing config.")

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent # Directory of this script
API_KEY_FILE = SCRIPT_DIR / API_KEY_FILENAME # API key expected in same dir as this script
OUTPUT_DIR = INPUT_PATH # Save Setlists.csv to the configured INPUT_PATH
ORIGINAL_OUTPUT_CSV = OUTPUT_DIR / SETLISTS_FILENAME
OUTPUT_CSV_FOR_TESTING = OUTPUT_DIR / TEST_SETLISTS_OUTPUT_FILENAME

# --- Load API Key ---
if not API_KEY_FILE.is_file(): logger.critical(f"API key file not found: {API_KEY_FILE}"); raise SystemExit("ERROR")
try:
    with open(API_KEY_FILE, "r", encoding="utf-8") as f: api_key = f.read().strip()
    if not api_key: logger.critical("API key file is empty."); raise SystemExit("ERROR")
except Exception as e: logger.critical(f"Failed to read API key: {e}"); raise SystemExit("ERROR")
headers = {"Accept": "application/json", "x-api-key": api_key}

# --- Process JSON Function (Unchanged) ---
def process_setlist_json(s: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # (Logic remains the same as artifact setlist_finder_async)
    try:
        artist_name = s.get("artist", {}).get("name", "")
        tour_name = s.get("tour", {}).get("name", "")
        event_date_str = s.get("eventDate", "")
        venue_info = s.get("venue", {}); venue_name = venue_info.get("name", "")
        city_info = venue_info.get("city", {}); city_name = city_info.get("name", "")
        state_code = city_info.get("stateCode", "")
        state_name = city_info.get("state", "")
        country_info = city_info.get("country", {}); country_name = country_info.get("name", ""); country_code = country_info.get("code", "")
        show_info_note = (s.get("info") or "").strip()
        output_date_str = event_date_str
        try: dt = datetime.strptime(event_date_str, "%d-%m-%Y"); output_date_str = dt.strftime("%m/%d/%Y")
        except ValueError: pass
        songs, song_notes = [], {}
        sets_obj = s.get("sets", {}).get("set", [])
        if isinstance(sets_obj, list):
            for set_block in sets_obj:
                if isinstance(set_block, dict):
                    song_list_info = set_block.get("song", [])
                    if isinstance(song_list_info, list):
                        for songinfo in song_list_info:
                            if isinstance(songinfo, dict):
                                name = (songinfo.get("name") or "").strip();
                                if name:
                                    songs.append(name)
                                    song_note = (songinfo.get("info") or "").strip()
                                    if song_note:
                                        if name not in song_notes: song_notes[name] = []
                                        song_notes[name].append(song_note)
        return {
            "Artist": artist_name, "Tour": tour_name,
            "Date": output_date_str, "ApiDate": event_date_str, "City": city_name, "Venue": venue_name,
            "StateCode": state_code, "StateName": state_name, "CountryCode": country_code, "CountryName": country_name,
            "Songs": songs, "_SongNotes": song_notes,
            "ShowInfo": show_info_note
        }
    except Exception as e: logger.error(f"Error processing setlist JSON: {e} - Data: {s}", exc_info=True); return None

# --- Async Fetching Logic & Status Update (Uses Config Variables) ---
last_log_time = time.time(); fetching_active = False
class LastLogTimeHandler(logging.Handler):
    def emit(self, record): global last_log_time; last_log_time = time.time(); pass
log_time_handler = LastLogTimeHandler(); logger.addHandler(log_time_handler)

async def status_updater(status_interval: float, silence_threshold: float):
    # (Logic unchanged)
    global last_log_time, fetching_active
    while fetching_active:
        await asyncio.sleep(status_interval)
        if not fetching_active: break
        now = time.time(); time_since_last_log = now - last_log_time
        if time_since_last_log > silence_threshold: logger.info(f"Still working... ({time_since_last_log:.0f}s)")

async def fetch_page(session: aiohttp.ClientSession, page: int, semaphore: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
    # (Logic unchanged, uses config vars: BRUCE_MBID, SETLISTFM_BASE_URL, REQUEST_TIMEOUT, MAX_RETRIES_PER_PAGE, SUCCESS_DELAY)
    params = {"artistMbid": BRUCE_MBID, "p": page}; retries = 0
    async with semaphore:
        while retries < MAX_RETRIES_PER_PAGE:
            try:
                logger.debug(f"Fetching page {page} (Attempt {retries+1})...")
                async with session.get(SETLISTFM_BASE_URL, params=params, timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json(); logger.debug(f"OK page {page}."); await asyncio.sleep(SUCCESS_DELAY); return data
                    elif resp.status == 429: logger.warning(f"429 page {page}, attempt {retries+1}. Wait 5s..."); retries+=1; await asyncio.sleep(5)
                    elif resp.status == 404 and page > 1: logger.info(f"404 page {page}. End."); return {"setlist": []}
                    else: logger.error(f"HTTP {resp.status} page {page}."); return None
            except asyncio.TimeoutError: logger.warning(f"Timeout page {page}, attempt {retries+1}."); retries+=1; await asyncio.sleep(2)
            except aiohttp.ClientError as e: logger.error(f"Client error page {page}: {e}"); return None
            except Exception as e: logger.error(f"Fetch error page {page}: {e}", exc_info=True); return None
        logger.error(f"Failed fetch page {page} after {MAX_RETRIES_PER_PAGE} retries."); return None

async def fetch_all_springsteen_setlists_async(test_mode_year: Optional[int] = None, max_test_pages: int = float('inf')) -> List[Dict[str, Any]]:
    # (Logic unchanged, uses config vars: MAX_CONCURRENT_REQUESTS, STATUS_UPDATE_INTERVAL, SILENCE_THRESHOLD)
    global last_log_time, fetching_active
    all_processed_shows: List[Dict[str, Any]] = []
    total_pages = 1; total_items = 0; is_test_mode = test_mode_year is not None
    page_limit = max_test_pages if is_test_mode else float('inf')
    processed_page_count = 0; failed_page_count = 0
    last_log_time = time.time(); fetching_active = True
    status_task = asyncio.create_task(status_updater(STATUS_UPDATE_INTERVAL, SILENCE_THRESHOLD))
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2); timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 5)
    try:
        async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
            logger.info("Fetching page 1..."); page1_data = await fetch_page(session, 1, semaphore)
            if page1_data is None: logger.critical("Failed page 1."); return []
            total_items = page1_data.get("total", 0); items_pp = page1_data.get("itemsPerPage", 20)
            total_pages = (total_items + items_pp - 1) // items_pp if items_pp > 0 else (1 if total_items > 0 else 0)
            logger.info(f"API: {total_items} items / ~{total_pages} pages.")
            page1_setlists = page1_data.get("setlist", []); page1_shows_added = 0
            if page1_setlists:
                for s in page1_setlists:
                     processed = process_setlist_json(s)
                     if processed:
                         if is_test_mode:
                             try: event_dt = datetime.strptime(processed["ApiDate"], "%d-%m-%Y");
                             except ValueError: continue
                             if event_dt.year != test_mode_year: continue
                         all_processed_shows.append(processed); page1_shows_added += 1
                processed_page_count += 1; logger.info(f"Processed page 1, added {page1_shows_added} shows.")
            else: logger.warning("No setlists page 1."); return []
            actual_limit = total_pages
            if page_limit != float('inf'): actual_limit = min(total_pages, int(page_limit)); logger.info(f"Page limit: {actual_limit}")
            tasks = []
            if actual_limit > 1:
                logger.info(f"Creating tasks: 2 - {actual_limit}...");
                tasks = [asyncio.create_task(fetch_page(session, page_num, semaphore)) for page_num in range(2, actual_limit + 1)]
                logger.info(f"Running {len(tasks)} tasks (max {MAX_CONCURRENT_REQUESTS})..."); results = await asyncio.gather(*tasks)
                logger.info("Processing results...");
                for i, page_data in enumerate(results):
                    page_num_actual = i + 2
                    if page_data and "setlist" in page_data:
                        processed_page_count += 1; page_shows_added = 0; current_setlists = page_data.get("setlist", [])
                        if not current_setlists and processed_page_count < actual_limit: logger.warning(f"Empty list page {page_num_actual}.")
                        for s in current_setlists:
                            processed = process_setlist_json(s)
                            if processed:
                                if is_test_mode:
                                     try: event_dt = datetime.strptime(processed["ApiDate"], "%d-%m-%Y");
                                     except ValueError: continue
                                     if event_dt.year != test_mode_year: continue
                                all_processed_shows.append(processed); page_shows_added += 1
                        logger.debug(f"Processed page {page_num_actual}, added {page_shows_added} shows.")
                    else: failed_page_count += 1; logger.warning(f"Page {page_num_actual} failed.")
    finally:
        fetching_active = False; status_task.cancel(); await asyncio.sleep(0)
    total_collected = len(all_processed_shows)
    logger.info(f"Finished fetch. Processed {processed_page_count} pages.")
    if failed_page_count > 0: logger.warning(f"{failed_page_count} pages failed.")
    logger.info(f"Collected {total_collected} total shows."); return all_processed_shows

# --- Other functions (write_to_flat_csv, fetch_setlist_by_date, add_setlist_to_data) ---
# (Unchanged - they use the paths defined at the top which now come from config)
def write_to_flat_csv(shows: List[Dict[str, Any]], output_file_path: Path):
    # (Logic unchanged)
    output_file = Path(output_file_path); logger.info(f"Preparing {len(shows)} shows for CSV: {output_file}"); all_rows, rows_with_no_songs = [], 0
    output_columns = ["Date", "ApiDate", "Tour", "City", "Venue", "StateCode", "StateName", "CountryCode", "CountryName", "Position", "Song", "Notes", "ShowNotes"]
    for show in shows:
        song_list = show.get("Songs", []); show_notes = show.get("_SongNotes", {}); show_info_note = show.get("ShowInfo", "")
        base_row_data = { "Date": show.get("Date", ""), "ApiDate": show.get("ApiDate", ""), "Tour": show.get("Tour", ""), "City": show.get("City", ""), "Venue": show.get("Venue", ""), "StateCode": show.get("StateCode", ""), "StateName": show.get("StateName", ""), "CountryCode": show.get("CountryCode", ""), "CountryName": show.get("CountryName", ""), "ShowNotes": show_info_note }
        if not song_list:
            rows_with_no_songs += 1; row = base_row_data.copy(); row.update({"Position": 0, "Song": "", "Notes": ""}); all_rows.append(row)
        else:
            temp_notes = show_notes.copy()
            for i, song_name in enumerate(song_list, 1):
                song_specific_notes = "; ".join(temp_notes.get(song_name, []));
                if song_name in temp_notes and temp_notes[song_name]:
                    try: temp_notes[song_name].pop(0)
                    except IndexError: pass
                row = base_row_data.copy(); row.update({"Position": i, "Song": song_name, "Notes": song_specific_notes}); all_rows.append(row)
    if rows_with_no_songs > 0: logger.warning(f"Created placeholders for {rows_with_no_songs} shows with no songs.")
    if not all_rows: logger.warning("No data rows generated."); return False
    df = pd.DataFrame(all_rows);
    for col in output_columns:
        if col not in df.columns: df[col] = ""
    df = df[output_columns]
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True); df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"Successfully wrote {len(all_rows)} rows to {output_file}"); return True
    except Exception as e: logger.error(f"Failed to write CSV {output_file}: {e}"); return False

def fetch_setlist_by_date(date_str, format_in="%m/%d/%Y", format_out="%d-%m-%Y"):
    # (Logic unchanged, uses config vars)
    try: dt = datetime.strptime(date_str, format_in); api_date = dt.strftime(format_out)
    except ValueError: logger.error(f"Invalid date format '{date_str}'."); return None
    params = {"artistMbid": BRUCE_MBID, "date": api_date}; logger.info(f"Fetching setlist for {date_str}...")
    try: resp = requests.get(SETLISTFM_BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT); resp.raise_for_status(); data = resp.json()
    except Exception as e: logger.error(f"Failed fetch for {date_str}: {e}"); return None
    setlists = data.get("setlist", []);
    if not setlists: logger.warning(f"No setlist found for {date_str}"); return None
    show_dict = process_setlist_json(setlists[0]); logger.info(f"Parsed setlist for {date_str}."); return show_dict

def add_setlist_to_data(setlist: Dict[str, Any], output_file_path: Path) -> bool:
    # (Logic unchanged)
    output_file = Path(output_file_path);
    if not setlist: logger.error("No setlist data provided."); return False
    expected_columns = ["Date", "ApiDate", "Tour", "City", "Venue", "StateCode", "StateName", "CountryCode", "CountryName", "Position", "Song", "Notes", "ShowNotes"]
    try:
        if output_file.is_file() and output_file.stat().st_size > 0:
            try: existing_df = pd.read_csv(output_file, encoding='utf-8-sig', keep_default_na=False)
            except pd.errors.EmptyDataError: logger.warning(f"{output_file} empty."); existing_df = pd.DataFrame(columns=expected_columns)
        else: existing_df = pd.DataFrame(columns=expected_columns)
        for col in expected_columns:
            if col not in existing_df.columns: logger.warning(f"Adding missing column '{col}'."); existing_df[col] = ""
        new_rows = []; show_date = setlist.get("Date", ""); show_venue = setlist.get("Venue", "")
        show_songs = setlist.get("Songs", []); show_notes_dict = setlist.get("_SongNotes", {}); show_info_note = setlist.get("ShowInfo", "")
        base_row_data = { "Date": show_date, "ApiDate": setlist.get("ApiDate", ""), "Tour": setlist.get("Tour", ""), "City": setlist.get("City", ""), "Venue": show_venue, "StateCode": setlist.get("StateCode", ""), "StateName": setlist.get("StateName", ""), "CountryCode": setlist.get("CountryCode", ""), "CountryName": setlist.get("CountryName", ""), "ShowNotes": show_info_note }
        if not show_songs:
            if existing_df[(existing_df["Date"] == show_date) & (existing_df["Venue"] == show_venue) & (existing_df["Position"].astype(str) == '0')].empty:
                row = base_row_data.copy(); row.update({"Position": 0, "Song": "", "Notes": "" }); new_rows.append(row)
            else: logger.info(f"Placeholder exists: {show_date} - {show_venue}")
        else:
            temp_notes = show_notes_dict.copy()
            for i, song_name in enumerate(show_songs, 1):
                if not existing_df[(existing_df["Date"] == show_date) & (existing_df["Venue"] == show_venue) & (existing_df["Position"].astype(str) == str(i)) & (existing_df["Song"] == song_name)].empty:
                    logger.debug(f"Entry exists: {show_date}, Pos {i}: {song_name}"); continue
                song_specific_notes = "";
                if song_name in temp_notes and temp_notes[song_name]:
                    try: song_specific_notes = temp_notes[song_name].pop(0)
                    except IndexError: pass
                row = base_row_data.copy(); row.update({"Position": i, "Song": song_name, "Notes": song_specific_notes }); new_rows.append(row)
        if new_rows:
            new_df = pd.DataFrame(new_rows);
            for col in expected_columns:
                if col not in new_df.columns: new_df[col] = ""
            new_df = new_df[expected_columns]
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            try:
                combined_df["_sort_date"] = pd.to_datetime(combined_df["Date"], format="%m/%d/%Y", errors='coerce')
                combined_df["_sort_pos"] = pd.to_numeric(combined_df["Position"], errors='coerce').fillna(0)
                combined_df = combined_df.sort_values(by=["_sort_date", "_sort_pos"], na_position='last')
                combined_df = combined_df.drop(columns=["_sort_date", "_sort_pos"])
            except Exception as e: logger.warning(f"Could not sort combined data: {e}.")
            combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Added {len(new_rows)} new entries to {output_file}. Total: {len(combined_df)}.")
            return True
        else: logger.info("No new entries to add."); return True
    except Exception as e: logger.error(f"Error adding setlist to {output_file}: {e}", exc_info=True); return False

# --- Main execution block (Unchanged) ---
# (Argument parsing and function calls unchanged)
# Wrapper functions to run async code from sync main block
def run_full_update_async_wrapper(output_target_file):
    logger.info(f"--- Starting FULL ASYNC update -> {output_target_file} ---")
    output_target_file.parent.mkdir(parents=True, exist_ok=True)
    shows = asyncio.run(fetch_all_springsteen_setlists_async(test_mode_year=None))
    if not shows: logger.error("Failed async fetch."); return
    logger.info("Sorting shows by date...");
    def parse_mdY_safe(d_str): 
        try: return datetime.strptime(d_str, "%m/%d/%Y") 
        except: 
            return datetime.max
    shows.sort(key=lambda s: parse_mdY_safe(s.get("Date", "")))
    success = write_to_flat_csv(shows, output_target_file)
    if success: logger.info(f"\n--- Full Async Update Done! -> {output_target_file} ---")
    else: logger.error("\n--- Full Async Update FAILED ---")

def run_test_update_async_wrapper(output_target_file):
    logger.info(f"--- Starting TEST ASYNC update -> {output_target_file} ---")
    output_target_file.parent.mkdir(parents=True, exist_ok=True)
    shows = asyncio.run(fetch_all_springsteen_setlists_async(test_mode_year=2024, max_test_pages=2))
    if not shows: logger.error("Failed async test fetch."); return
    logger.info("Sorting shows by date...");
    def parse_mdY_safe(d_str): 
        try: return datetime.strptime(d_str, "%m/%d/%Y") 
        except: 
            return datetime.max
    shows.sort(key=lambda s: parse_mdY_safe(s.get("Date", "")))
    success = write_to_flat_csv(shows, output_target_file)
    if success: logger.info(f"\n--- TEST Async Update Done! -> {output_target_file} ---")
    else: logger.error("\n--- TEST Async Update FAILED ---")

def run_fetch_specific_show(date_str, output_target_file):
    logger.info(f"--- Fetching specific show: {date_str} -> {output_target_file} ---")
    output_target_file.parent.mkdir(parents=True, exist_ok=True)
    setlist_data = fetch_setlist_by_date(date_str)
    if setlist_data:
        success = add_setlist_to_data(setlist_data, output_target_file)
        if not success: logger.error(f"Failed add setlist from {date_str}.")
    else: logger.error(f"Could not fetch/parse {date_str}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch RAW Bruce Springsteen setlists from Setlist.fm CONCURRENTLY.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--full-update", action="store_true", help=f"Fetch ALL setlists concurrently, writing RAW data to MAIN file ({SETLISTS_FILENAME}). Overwrites.")
    parser.add_argument("--fetch-date", metavar="MM/DD/YYYY", help=f"Fetch specific date and add/update RAW data in MAIN file ({SETLISTS_FILENAME}).")
    parser.add_argument("--test", action="store_true", help=f"TEST mode: Fetch limited 2024 data concurrently to TEST file ({TEST_SETLISTS_OUTPUT_FILENAME}). Overwrites.")
    parser.add_argument("--output-file", metavar="FILENAME.csv", default=None, help=f"Specify output CSV filename (in {OUTPUT_DIR}). Used by --full-update/--fetch-date if provided. DEFAULT is {SETLISTS_FILENAME}. Ignored if --test.")
    args = parser.parse_args()
    if args.test:
        target_output_file = OUTPUT_CSV_FOR_TESTING; is_test_run = True; is_full_run = False; is_fetch_date = False
        logger.info(f"--- TEST MODE (Concurrent) -> {target_output_file} ---")
    elif args.full_update:
        target_output_file = OUTPUT_DIR / args.output_file if args.output_file else ORIGINAL_OUTPUT_CSV
        is_test_run = False; is_full_run = True; is_fetch_date = False
        logger.info(f"Full update (Concurrent) -> {target_output_file}")
    elif args.fetch_date:
        target_output_file = OUTPUT_DIR / args.output_file if args.output_file else ORIGINAL_OUTPUT_CSV
        is_test_run = False; is_full_run = False; is_fetch_date = True
        logger.info(f"Fetch date -> {target_output_file}")
    else: parser.print_help(); sys.exit(0)
    if is_test_run:
        logger.info(f"Test mode will overwrite {target_output_file}.")
        run_test_update_async_wrapper(target_output_file)
    elif is_fetch_date:
        logger.info(f"Adding/Updating RAW data in {target_output_file} for {args.fetch_date}.")
        run_fetch_specific_show(args.fetch_date, target_output_file)
    elif is_full_run:
        confirm = input(f"WARNING: --full-update will fetch ALL setlists and OVERWRITE {target_output_file}. Continue? (yes/no): ").lower().strip()
        if confirm == 'yes': run_full_update_async_wrapper(target_output_file)
        else: logger.info("Full update cancelled.")


