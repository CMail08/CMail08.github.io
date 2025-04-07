# normalize_data.py
# Implements Ordered Word Sequence Matching strategy.
# Uses simplified config.
# UPDATED: Final shows columns reordered to match latest schema.
# FIX: Added missing 'import sys'.

import pandas as pd
import os
import re
import logging
from datetime import datetime
from typing import Optional, Dict, Tuple, List, Set, Any
import collections
import sys # <-- ADDED IMPORT

# Import from simplified config
from config import (
    SCRIPT_DIR, BASE_PATH, INPUT_PATH, OUTPUT_PATH,
    SONGS_SESSIONS_FILENAME, SETLISTS_FILENAME,
    SONGS_OUTPUT_FILENAME, SHOWS_OUTPUT_FILENAME, SETLISTS_OUTPUT_FILENAME,
    SCHEMA_SQL_FILE,
    MINIMAL_REPLACEMENTS, RE_WHITESPACE,
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# --- Helper Functions ---

def preprocess_title_for_matching(title: Optional[Any]) -> List[str]:
    """ Minimal cleanup and splits into a list of words for matching. """
    if pd.isna(title) or not title: return []
    # Revert to original logic: lowercase, replacements, word extraction
    title_str = str(title).lower()
    for old, new in MINIMAL_REPLACEMENTS.items(): title_str = title_str.replace(old, new)
    words = re.findall(r'\b[\w\']+\b', title_str)
    return words

def get_canonical_display_title(title: Optional[Any]) -> str:
    """ Creates a reasonably clean display title (basic Title Case). """
    if pd.isna(title) or not title: return "Unknown Title"
    # Revert to original logic: replacements, whitespace split, capitalize, specific fixes
    title_str = str(title)
    for old, new in MINIMAL_REPLACEMENTS.items(): title_str = title_str.replace(old, new)
    words = RE_WHITESPACE.split(title_str.strip())
    display_title = ' '.join(word.capitalize() for word in words if word)
    display_title = RE_WHITESPACE.sub(' ', display_title).strip()
    if display_title == "4Th Of July Asbury Park Sandy": display_title = "4th Of July Asbury Park (Sandy)"
    if display_title == "Born In The Usa": display_title = "Born In The U.S.A."
    return display_title if display_title else "Unknown Title"

def get_album_from_session(session: Optional[str]) -> str:
    """ Returns the raw session name, stripped of whitespace, with specific correction for BITUSA. """
    if pd.isna(session): return "Unknown"
    album_name = RE_WHITESPACE.sub(' ', str(session)).strip()
    # Specific correction for the album name (case-insensitive check)
    if album_name.lower() == "born in the usa":
        album_name = "Born in the U.S.A."
    return album_name

def is_subsequence(needle: List[str], haystack: List[str]) -> bool:
    """ Checks if needle is an ordered subsequence of haystack. """
    it = iter(haystack)
    return all(c in it for c in needle)

def find_match_by_sequence(raw_title_words: List[str], canonical_map: Dict[int, List[str]]) -> Optional[int]:
    """ Finds the best match using ordered subsequence and longest match tie-breaking. """
    if not raw_title_words: return None
    matches = []
    for song_id, canonical_words in canonical_map.items():
        if not canonical_words: continue
        if is_subsequence(canonical_words, raw_title_words):
            matches.append((len(canonical_words), song_id))
    if not matches: return None
    matches.sort(key=lambda x: x[0], reverse=True)
    return matches[0][1]

# --- Main Data Processing Functions ---

def create_final_songs_table(songs_sessions_df: pd.DataFrame, setlists_input_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, List[str]]]:
    """ Creates the final songs table and the matching map {song_id: word_list}. """
    logger.info("Consolidating unique songs from sessions and setlists...")
    all_songs_data: List[Dict[str, Any]] = []
    seen_preprocessed_words: Set[Tuple[str, ...]] = set()

    logger.info("Processing songs from sessions file...")
    for _, row in songs_sessions_df.iterrows():
        title_raw = row.get('song', '')
        if pd.isna(title_raw) or not title_raw: continue
        processed_words = tuple(preprocess_title_for_matching(title_raw))
        display_title = get_canonical_display_title(title_raw)
        if not processed_words or not display_title: continue
        if processed_words in seen_preprocessed_words: continue
        album_name = get_album_from_session(row.get('session', ''))
        is_outtake = 1 if str(row.get('type', '')).lower() == 'outtake' else 0
        all_songs_data.append({
            'title': display_title, 'album': album_name, 'is_outtake': is_outtake,
            '_processed_words': list(processed_words)
        })
        seen_preprocessed_words.add(processed_words)
    initial_songs_df = pd.DataFrame(all_songs_data)
    logger.info(f"Processed {len(initial_songs_df)} unique songs from sessions file.")
    known_processed_words = set(tuple(words) for words in initial_songs_df['_processed_words'])

    logger.info("Identifying additional songs present only in setlists...")
    missing_songs_map: Dict[Tuple[str, ...], Set[str]] = collections.defaultdict(set)
    input_song_col = 'song'
    if input_song_col not in setlists_input_df.columns:
        logger.error(f"Column '{input_song_col}' not found in setlist input. Cannot find missing songs.")
    else:
        unique_setlist_titles = setlists_input_df[input_song_col].drop_duplicates()
        for title_raw in unique_setlist_titles:
            if pd.isna(title_raw) or not title_raw: continue
            processed_words = tuple(preprocess_title_for_matching(title_raw))
            if not processed_words: continue
            if processed_words not in known_processed_words:
                missing_songs_map[processed_words].add(str(title_raw))

    new_songs_data = []
    if missing_songs_map:
        logger.info(f"Found {len(missing_songs_map)} unique word sequences in setlists not present in sessions (assumed covers).")
        for processed_words, original_titles in missing_songs_map.items():
            representative_raw = min(original_titles, key=len)
            display_title = get_canonical_display_title(representative_raw)
            if not display_title: continue
            new_songs_data.append({
                'title': display_title, 'album': 'Cover', 'is_outtake': 0,
                '_processed_words': list(processed_words)
            })
            seen_preprocessed_words.add(processed_words)
            known_processed_words.add(processed_words)

    new_songs_df = pd.DataFrame(new_songs_data)
    final_songs_df = pd.concat([initial_songs_df, new_songs_df], ignore_index=True)
    final_songs_df['_processed_tuple'] = final_songs_df['_processed_words'].apply(tuple)
    final_songs_df = final_songs_df.drop_duplicates(subset=['_processed_tuple'], keep='first')
    final_songs_df.insert(0, 'song_id', range(1, len(final_songs_df) + 1))
    songs_table_final = final_songs_df[['song_id', 'title', 'album', 'is_outtake']]
    logger.info(f"Created final songs table with {len(songs_table_final)} unique songs.")
    matching_map = pd.Series(final_songs_df['_processed_words'].values, index=final_songs_df['song_id']).to_dict()
    return songs_table_final, matching_map


def create_shows_setlists_tables(setlists_input_df: pd.DataFrame, songs_df_final: pd.DataFrame, canonical_matching_map: Dict[int, List[str]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """ Creates shows and setlists tables using sequence matching and new schema. """
    logger.info("Processing raw setlist data to create shows and setlists tables...")
    shows_data: List[Dict] = []; show_keys: Set[Tuple[Optional[str], str]] = set()
    original_show_key_to_id_map: Dict[Tuple[str, str], int] = {}; show_id_counter = 1
    skipped_duplicate_shows = 0; tours_filled_by_year = 0
    logger.info("Identifying unique shows and determining tour/location/notes...")
    processed_show_rows = 0;
    expected_input_cols = ['date', 'venue', 'tour', 'city', 'statecode', 'statename', 'countrycode', 'countryname', 'apidate', 'shownotes', 'song', 'position', 'notes']
    for col in expected_input_cols:
        if col not in setlists_input_df.columns:
             logger.warning(f"Expected input column '{col}' not found in CSV '{SETLISTS_FILENAME}'. Using default empty value.")
             setlists_input_df[col] = ""
    for index, row in setlists_input_df.iterrows():
        processed_show_rows += 1;
        if processed_show_rows % 20000 == 0: logger.debug(f"Scanning input row {processed_show_rows} for shows...")
        original_date_str=str(row.get('date', '')).strip()
        original_venue_str=str(row.get('venue', '')).strip()
        if not original_date_str or not original_venue_str: continue
        db_date_str: Optional[str]=None; output_year: Optional[str]=None
        try:
            date_obj=pd.to_datetime(original_date_str, format='%m/%d/%Y', errors='coerce')
            if pd.notna(date_obj):
                db_date_str=date_obj.strftime('%Y-%m-%d')
                output_year=date_obj.strftime('%Y')
        except Exception: pass
        venue_str=original_venue_str if original_venue_str else 'Unknown Venue'
        unique_show_key=(db_date_str, venue_str)
        if unique_show_key in show_keys:
            skipped_duplicate_shows += 1
            continue
        show_keys.add(unique_show_key); current_show_id=show_id_counter
        raw_tour_name=str(row.get('tour', '')).strip()
        raw_city=str(row.get('city', '')).strip()
        raw_state_code=str(row.get('statecode', '')).strip()
        raw_state_name=str(row.get('statename', '')).strip()
        raw_country_code=str(row.get('countrycode', '')).strip()
        raw_country_name=str(row.get('countryname', '')).strip()
        raw_show_notes = str(row.get('shownotes', '')).strip()
        final_tour_name = raw_tour_name
        if not final_tour_name and output_year:
            final_tour_name = output_year
            tours_filled_by_year += 1
        elif not final_tour_name:
            final_tour_name = "Unknown Tour"
        shows_data.append({
            'show_id': current_show_id, 'date': db_date_str, 'tour': final_tour_name,
            'venue': venue_str, # New Order
            'city': raw_city if raw_city else 'Unknown City', # New Order
            'state_name': raw_state_name if raw_state_name else None, # New Order
            'state_code': raw_state_code if raw_state_code else None, # New Order
            'country_name': raw_country_name if raw_country_name else None, # New Order
            'country_code': raw_country_code if raw_country_code else None, # New Order
            'show_notes': raw_show_notes if raw_show_notes else None,
        })
        original_input_key=(original_date_str, original_venue_str)
        if original_input_key not in original_show_key_to_id_map:
             original_show_key_to_id_map[original_input_key]=current_show_id
        show_id_counter += 1
    shows_df=pd.DataFrame(shows_data)
    if skipped_duplicate_shows > 0: logger.info(f"Skipped {skipped_duplicate_shows} duplicate show entries based on date/venue.")
    if tours_filled_by_year > 0: logger.info(f"Filled {tours_filled_by_year} empty tours using year.")
    logger.info(f"Created unique shows list: {len(shows_df)} shows.")

    logger.info("Building final show mapping using original date/venue keys...")
    final_show_mapping: Dict[Tuple[str, str], int] = {}
    final_show_mapping = original_show_key_to_id_map
    if len(final_show_mapping) != len(shows_df):
         logger.warning(f"Show mapping size ({len(final_show_mapping)}) differs from unique shows found ({len(shows_df)}). Check key logic.")
    logger.info(f"Finished building final show mapping with {len(final_show_mapping)} entries.")

    # --- Pre-process BITUSA titles for contextual check ---
    # *** User needs to confirm this raw title for the acoustic version ***
    reunion_bitusa_lookup_title_raw = "Born In The USA (Acoustic)"
    # ---
    standard_bitusa_words = preprocess_title_for_matching("Born In The USA")
    reunion_bitusa_words = preprocess_title_for_matching(reunion_bitusa_lookup_title_raw)
    standard_bitusa_id: Optional[int] = None
    reunion_bitusa_id: Optional[int] = None
    for s_id, words_list in canonical_matching_map.items():
         if words_list == standard_bitusa_words: standard_bitusa_id = s_id
         elif words_list == reunion_bitusa_words: reunion_bitusa_id = s_id
    if standard_bitusa_id: logger.info(f"Found standard BITUSA song_id: {standard_bitusa_id} for words {standard_bitusa_words}")
    else: logger.warning(f"Could not find song_id for standard BITUSA words: {standard_bitusa_words}")
    if reunion_bitusa_id: logger.info(f"Found Reunion/Acoustic BITUSA song_id: {reunion_bitusa_id} for words {reunion_bitusa_words}")
    else: logger.warning(f"Could not find song_id for Reunion/Acoustic version using lookup title '{reunion_bitusa_lookup_title_raw}' (words: {reunion_bitusa_words}). Contextual logic may fail.")

    show_id_to_tour_map = shows_df.set_index('show_id')['tour'].to_dict()

    # Create Setlists Table
    setlists: List[Dict] = []; logger.info("Creating final setlist entries using sequence matching...")
    setlist_entry_id_counter = 1; processed_count = 0; skipped_rows = 0;
    match_stats = collections.defaultdict(int)
    setlist_keys: Set[Tuple[int, int, int]] = set(); duplicate_setlist_entries_skipped = 0
    total_rows_setlist_input = len(setlists_input_df)
    song_col, pos_col, notes_col = 'song', 'position', 'notes'
    REUNION_TOUR_RAW_NAMES = {"reunion tour", "reunion"}

    for index, row in setlists_input_df.iterrows():
        processed_count += 1;
        if processed_count % 20000 == 0: logger.debug(f"Processing setlist row {processed_count}/{total_rows_setlist_input}")
        original_date_str = str(row.get('date', '')).strip(); original_venue_str = str(row.get('venue', '')).strip()
        original_input_key = (original_date_str, original_venue_str);
        show_id = final_show_mapping.get(original_input_key)
        song_title_raw = str(row.get(song_col, '')).strip();
        if not show_id:
            if original_date_str and original_venue_str: logger.debug(f"Skipping row - Could not find show_id for key: ('{original_date_str}', '{original_venue_str}')")
            skipped_rows += 1; continue
        if not song_title_raw: skipped_rows += 1; continue

        song_id_to_use: Optional[int] = None
        match_method: Optional[str] = "Sequence Match"
        raw_title_words = preprocess_title_for_matching(song_title_raw)
        if not raw_title_words: skipped_rows += 1; continue

        # Contextual Logic for BITUSA
        if raw_title_words == standard_bitusa_words and standard_bitusa_id is not None:
            tour_name = show_id_to_tour_map.get(show_id, "")
            if tour_name.lower() in REUNION_TOUR_RAW_NAMES and reunion_bitusa_id is not None:
                song_id_to_use = reunion_bitusa_id
                match_method = "Contextual (Reunion Tour)"
            else:
                song_id_to_use = standard_bitusa_id
                match_method = "Contextual (Standard BITUSA)"

        # Sequence Matching (if not handled above)
        if song_id_to_use is None:
            song_id_to_use = find_match_by_sequence(raw_title_words, canonical_matching_map)
            if song_id_to_use: match_method = "Sequence Match (Success)"
            else: match_method = "Sequence Match (Fail)"

        if not song_id_to_use:
            match_stats[match_method] += 1
            skipped_rows += 1; continue

        match_stats[match_method] += 1

        # Position, Duplicates, Notes
        position_val = row.get(pos_col);
        try: numeric_pos = pd.to_numeric(position_val, errors='coerce'); position = 0 if pd.isna(numeric_pos) else int(numeric_pos)
        except (ValueError, TypeError): position = 0
        setlist_key = (show_id, song_id_to_use, position)
        if setlist_key in setlist_keys:
            duplicate_setlist_entries_skipped += 1; continue
        setlist_keys.add(setlist_key)
        notes_raw = str(row.get(notes_col, '')).strip(); notes = "" if pd.isna(notes_raw) else notes_raw

        setlists.append({'setlist_entry_id': setlist_entry_id_counter, 'show_id': show_id, 'song_id': song_id_to_use, 'position': position, 'notes': notes})
        setlist_entry_id_counter += 1

    setlists_df_final = pd.DataFrame(setlists)
    if not setlists_df_final.empty: setlists_df_final = setlists_df_final[['setlist_entry_id', 'show_id', 'song_id', 'position', 'notes']]
    else: logger.warning("No setlist entries generated.")

    # Log Match Statistics
    logger.info("--- Song Matching Statistics ---")
    total_matched = sum(count for method, count in match_stats.items() if method != "Sequence Match (Fail)")
    total_failed = match_stats.get("Sequence Match (Fail)", 0)
    total_processed_with_song = processed_count - (skipped_rows - total_failed) # Rows where matching was attempted
    logger.info(f"Total setlist rows processed with song title: {total_processed_with_song}")
    logger.info(f"Total successfully matched to a song_id: {total_matched}")
    logger.info(f"Total failed to match: {total_failed}")
    logger.info(f"Total skipped (missing show map or missing song title): {skipped_rows - total_failed}")
    logger.info("Matches by method:")
    for method, count in sorted(match_stats.items()):
         logger.info(f"  - {method}: {count}")
    logger.info("--- End Matching Statistics ---")

    if duplicate_setlist_entries_skipped > 0: logger.warning(f"Skipped {duplicate_setlist_entries_skipped} duplicate setlist entries (same show, song, position).")
    logger.info(f"Generated {len(setlists_df_final)} final setlist entries.")

    # Define final columns for shows_df based on the new schema and desired order
    final_show_columns = [
        'show_id', 'date', 'tour',
        'venue', 'city', 'state_name', 'state_code', 'country_name', 'country_code', # Reordered location
        'show_notes' # Removed api_date
    ]
    for col in final_show_columns:
        if col not in shows_df.columns:
             shows_df[col] = None
    shows_df_final = shows_df[final_show_columns] # Select and reorder

    return shows_df_final, setlists_df_final


# ============================================================================
# Main Execution Logic
# ============================================================================
def normalize_database_schema():
    """Main function to load raw data, apply normalizations, and create output CSV files."""
    logger.info("Starting database schema normalization...")
    songs_sessions_file = INPUT_PATH / SONGS_SESSIONS_FILENAME
    setlists_raw_file = INPUT_PATH / SETLISTS_FILENAME
    songs_output_file = OUTPUT_PATH / SONGS_OUTPUT_FILENAME
    shows_output_file = OUTPUT_PATH / SHOWS_OUTPUT_FILENAME
    setlists_output_file = OUTPUT_PATH / SETLISTS_OUTPUT_FILENAME

    try:
        if not songs_sessions_file.exists(): raise FileNotFoundError(f"Songs/Sessions file not found: {songs_sessions_file}")
        if not setlists_raw_file.exists(): raise FileNotFoundError(f"Raw Setlists file not found: {setlists_raw_file}")
        logger.info(f"Loading {SONGS_SESSIONS_FILENAME}...")
        songs_sessions_df = pd.read_csv(songs_sessions_file, encoding='latin-1', keep_default_na=False)
        songs_sessions_df.columns = [c.strip().lower().replace(' ', '_') for c in songs_sessions_df.columns]
        songs_sessions_df.rename(columns={'song_title': 'song', 'album_or_outtake': 'type'}, inplace=True, errors='ignore')
        logger.info(f"Loading RAW {SETLISTS_FILENAME}...")
        setlists_input_df = pd.read_csv(setlists_raw_file, encoding='utf-8-sig', low_memory=False, keep_default_na=False)
        setlists_input_df.columns = [c.strip().lower().replace(' ', '_') for c in setlists_input_df.columns]
        logger.info(f"Loaded {len(setlists_input_df)} raw rows from {SETLISTS_FILENAME}.")
    except FileNotFoundError as e:
        logger.error(e);
        # --- ADDED sys.exit on critical file error ---
        sys.exit(f"Exiting: {e}")
    except Exception as e:
        logger.exception(f"Error loading input files: {e}");
        sys.exit("Exiting due to load error.")
        # ---

    songs_table_final, canonical_matching_map = create_final_songs_table(songs_sessions_df, setlists_input_df)
    shows_table_normalized, setlists_table_normalized = create_shows_setlists_tables(setlists_input_df, songs_table_final, canonical_matching_map)

    logger.info("Saving normalized CSV files...")
    try:
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
        songs_table_final.to_csv(songs_output_file, index=False, encoding='utf-8-sig')
        shows_table_normalized.to_csv(shows_output_file, index=False, encoding='utf-8-sig')
        setlists_table_normalized.to_csv(setlists_output_file, index=False, encoding='utf-8-sig')
        logger.info("Normalized database schema created successfully:")
        logger.info(f"- Songs table ({songs_output_file.name}): {len(songs_table_final)} rows")
        logger.info(f"- Shows table ({shows_output_file.name}): {len(shows_table_normalized)} rows")
        logger.info(f"- Setlists table ({setlists_output_file.name}): {len(setlists_table_normalized)} rows")
        logger.info(f"Files saved to {OUTPUT_PATH}")
    except Exception as e: logger.exception(f"Error saving normalized CSV files: {e}")

if __name__ == "__main__":
    normalize_database_schema()

