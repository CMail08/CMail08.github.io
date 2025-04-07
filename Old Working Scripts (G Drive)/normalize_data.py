# normalize_data
import pandas as pd
import os
import re
import logging
from datetime import datetime
from typing import Optional, Dict, Tuple, List, Set

# Import from config
from config import (
    SCRIPT_DIR, BASE_PATH, INPUT_PATH, OUTPUT_PATH,
    SONGS_SESSIONS_FILENAME, SETLISTS_FILENAME,
    SONGS_OUTPUT_FILENAME, SHOWS_OUTPUT_FILENAME, SETLISTS_OUTPUT_FILENAME,
    TITLE_REPLACEMENTS, RE_NON_ALPHANUMERIC, RE_WHITESPACE, TITLE_TYPO_CORRECTIONS,
    KNOWN_COVERS_RAW, CITY_TO_REGION_MAP
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress specific pandas warnings if necessary (optional)
# warnings.simplefilter(action='ignore', category=FutureWarning)

# Ensure the output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# --- Helper Functions ---

def normalize_title(title: Optional[str | float]) -> str:
    """
    Normalize a song title for better matching.
    Uses rules defined in config.py.
    """
    if pd.isna(title) or not title:
        return "" # Return empty string for NaN or empty input

    title_str = str(title).strip().upper() # Ensure string, strip whitespace, convert to uppercase

    # Replace common abbreviations and symbols first (from config)
    for old, new in TITLE_REPLACEMENTS.items():
        title_str = title_str.replace(old, new)

    # Remove remaining non-alphanumeric characters (except spaces)
    title_str = RE_NON_ALPHANUMERIC.sub('', title_str)
    # Normalize whitespace (multiple spaces to single space)
    title_str = RE_WHITESPACE.sub(' ', title_str).strip()

    # Handle known typos and variations (from config)
    # Exact match replacement for typos is generally safer
    if title_str in TITLE_TYPO_CORRECTIONS:
         title_str = TITLE_TYPO_CORRECTIONS[title_str]
    else:
        # Apply broader corrections if exact typo match not found
        # (This part could be refined if exact matches are preferred)
        for typo, correction in TITLE_TYPO_CORRECTIONS.items():
             # Use word boundaries if needed, but simple replace often works
             if typo in title_str: # Check substring if not exact match
                 title_str = title_str.replace(typo, correction)


    # Specific title normalizations (can be expanded or moved to config if complex)
    if "BUS STOP" in title_str and "STREET" in title_str:
        title_str = "DOES THIS BUS STOP AT 82ND STREET" # Normalize specific known title
    if title_str == "DOES THIS BUS STOP AT 8ND STREET": # Catch specific typo version
         title_str = "DOES THIS BUS STOP AT 82ND STREET"
    if "SANDY" in title_str or "4TH OF JULY" in title_str or "FOURTH OF JULY" in title_str:
        # Standardize the Sandy variations
        title_str = "4TH OF JULY ASBURY PARK SANDY"

    # Final check for specific common title after all normalizations
    # Example: Ensure Santa Claus ends correctly
    if title_str == "SANTA CLAUS IS COMIN TO TOWN":
        title_str = "SANTA CLAUS IS COMING TO TOWN"

    return title_str

def get_album_from_session(session: Optional[str]) -> str:
    """
    Map a recording session to its most likely album name.
    This provides a simplified mapping based on keywords.
    """
    if pd.isna(session):
        return "Unknown" # Default for missing session info

    session_str = str(session).strip().lower()

    # Simple mapping based on keywords in session names (can be refined/externalized)
    if 'greetings' in session_str: return 'Greetings From Asbury Park NJ'
    if 'wild' in session_str and 'innocent' in session_str: return 'The Wild The Innocent & The E Street Shuffle'
    if 'born to run' in session_str: return 'Born To Run'
    if 'darkness' in session_str: return 'Darkness On The Edge Of Town'
    if 'river' in session_str: return 'The River'
    if 'nebraska' in session_str: return 'Nebraska'
    if 'born in the usa' in session_str or 'born in the u.s.a' in session_str: return 'Born In The USA'
    if 'tunnel of love' in session_str: return 'Tunnel Of Love'
    if 'human touch' in session_str: return 'Human Touch'
    if 'lucky town' in session_str: return 'Lucky Town'
    if 'tom joad' in session_str: return 'The Ghost Of Tom Joad'
    if 'rising' in session_str: return 'The Rising'
    if 'devils' in session_str and 'dust' in session_str: return 'Devils and Dust'
    if 'seeger' in session_str: return 'The Seeger Sessions'
    if 'magic' in session_str: return 'Magic'
    if 'working on a dream' in session_str: return 'Working On A Dream'
    if 'wrecking ball' in session_str: return 'Wrecking Ball'
    if 'high hopes' in session_str: return 'High Hopes'
    if 'western stars' in session_str: return 'Western Stars'
    if 'letter to you' in session_str: return 'Letter To You'
    if 'only the strong survive' in session_str: return 'Only The Strong Survive'

    # If no specific album match, use the cleaned session name or a default
    # Capitalize first letter of each word for consistency
    return str(session).strip().title() if session else "Unknown"


def get_region(city: Optional[str], venue: Optional[str]) -> str:
    """
    Determine a broader geographical region based on city.
    Uses the CITY_TO_REGION_MAP from config.py.
    The 'venue' parameter is currently unused in the primary logic but kept for signature consistency.
    """
    if pd.isna(city): city = ''
    # if pd.isna(venue): venue = '' # Venue currently not used in mapping logic

    city_lower = str(city).lower().strip()

    # Lookup in the map, default to "Unknown" if not found
    return CITY_TO_REGION_MAP.get(city_lower, "Unknown")


def find_best_match(song_title: Optional[str], title_map: Dict[str, Tuple[str, int]]) -> Tuple[Optional[int], str]:
    """
    Find the best match for a song title using multiple techniques.
    Uses a precomputed map {normalized_title: (original_title, song_id)}.
    Returns song_id and the method used for matching.
    """
    if pd.isna(song_title) or not song_title:
        return None, "empty title"

    original_title = str(song_title).strip()
    normalized_input = normalize_title(original_title)

    # 1. Exact normalized match
    if normalized_input in title_map:
        return title_map[normalized_input][1], "normalized exact"

    # 2. Substring check (input in map key OR map key in input)
    # Prioritize longer matches to avoid incorrect partial matches (e.g., "Run" matching "Born to Run")
    best_match_id: Optional[int] = None
    best_match_len = -1
    match_method: Optional[str] = None

    for norm_map_title, (orig_db_title, song_id) in title_map.items():
        # Avoid trivial substring matches by ensuring the match is significant
        # Check if normalized input contains a known normalized title (at least 4 chars long)
        if len(norm_map_title) > 3 and norm_map_title in normalized_input:
            if len(norm_map_title) > best_match_len:
                best_match_id = song_id
                best_match_len = len(norm_map_title)
                match_method = f"substring db in input: {orig_db_title}"

        # Check if a known normalized title contains the normalized input (input at least 4 chars long)
        elif len(normalized_input) > 3 and normalized_input in norm_map_title:
             if len(normalized_input) > best_match_len:
                best_match_id = song_id
                best_match_len = len(normalized_input)
                match_method = f"substring input in db: {orig_db_title}"

    if best_match_id is not None:
         return best_match_id, match_method

    # 3. Word subset matching (optional, can be added if needed)
    # ... logic for word subset matching ...

    return None, "no match found"


def create_songs_table(songs_sessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a normalized songs table from the songs/sessions data.
    Adds known covers (from config) and handles specific variations.
    Returns a DataFrame for the songs table.
    """
    logger.info("Creating initial songs table from sessions data...")
    songs: List[Dict] = []
    seen_normalized_titles: Set[str] = set() # Track normalized titles to avoid duplicates

    # Add known cover songs first (from config)
    for title in KNOWN_COVERS_RAW:
        normalized_title = normalize_title(title)
        # Use the normalized title for the official 'title' if it's unique
        # If a different original version is preferred, adjust logic here
        # Using Title Case for the final display title
        final_display_title = normalized_title.title()

        if normalized_title not in seen_normalized_titles and normalized_title: # Ensure non-empty
            songs.append({
                'title': final_display_title,
                'album': 'Cover',
                'is_outtake': 0
            })
            seen_normalized_titles.add(normalized_title)

    # Process songs from the sessions/outtakes file
    processed_count = 0
    for _, row in songs_sessions_df.iterrows():
        processed_count += 1
        title = row.get('song', '')
        if pd.isna(title) or not title:
            continue

        title_str = str(title).strip()
        # Handle specific markers if needed (e.g., ' *' for alternate versions)
        # Example: is_alternate = title_str.endswith(' *')
        # clean_title = title_str.removesuffix(' *').strip() if is_alternate else title_str
        clean_title = title_str # Simplified for now

        # Normalize the cleaned title
        normalized_title = normalize_title(clean_title)

        # Skip if empty after normalization or already added
        if not normalized_title or normalized_title in seen_normalized_titles:
            continue

        # Determine album and outtake status
        album_name = get_album_from_session(row.get('session', ''))
        is_outtake = 1 if str(row.get('type', '')).lower() == 'outtake' else 0

        # Use title case for the final title display
        final_display_title = clean_title.title()

        songs.append({
            'title': final_display_title,
            'album': album_name,
            'is_outtake': is_outtake
        })
        seen_normalized_titles.add(normalized_title)
        if processed_count % 500 == 0:
            logger.debug(f"Processed {processed_count} songs from sessions file...")

    # Create DataFrame and add song_id (starting from 1)
    songs_df = pd.DataFrame(songs)
    # Final deduplication based on the potentially more consistent display title
    songs_df = songs_df.drop_duplicates(subset=['title'])
    songs_df.insert(0, 'song_id', range(1, len(songs_df) + 1))

    # Reorder columns for clarity
    songs_df = songs_df[['song_id', 'title', 'album', 'is_outtake']]

    logger.info(f"Created initial songs table with {len(songs_df)} unique songs.")
    return songs_df


def create_shows_setlists_tables(setlists_input_df: pd.DataFrame, songs_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Create normalized shows and setlists tables from the raw setlists data.
    Adds missing songs (assumed covers) to the songs table.
    Adds a 'region' column derived from city/venue using config map.
    Returns:
        - shows_df: DataFrame for the shows table.
        - setlists_df: DataFrame for the setlists table.
        - updated_songs_df: DataFrame for the songs table including added covers.
    """
    logger.info("Processing setlist data to create shows and setlists tables...")

    # --- Create Shows Table ---
    shows: List[Dict] = []
    show_keys: Set[Tuple[str, str]] = set() # To track unique shows (date, venue)

    # Ensure required columns are present
    required_show_cols = ['date', 'tour', 'city', 'venue']
    for col in required_show_cols:
        if col not in setlists_input_df.columns:
            logger.warning(f"Column '{col}' not found in setlists input. Adding empty column.")
            setlists_input_df[col] = ""

    # Iterate through setlist input to find unique shows
    logger.info("Identifying unique shows...")
    processed_show_rows = 0
    for _, row in setlists_input_df.iterrows():
        processed_show_rows += 1
        if processed_show_rows % 20000 == 0:
             logger.debug(f"Scanning setlist row {processed_show_rows} for shows...")

        date_str = str(row.get('date', '')).strip()
        venue_str = str(row.get('venue', '')).strip()

        # Basic validation: skip if date or venue is missing
        if not date_str or not venue_str:
            continue

        show_key = (date_str, venue_str)
        if show_key in show_keys:
            continue # Skip already added show

        # Attempt to parse and format the date consistently (YYYY-MM-DD)
        db_date_str: Optional[str] = None
        try:
            # Let pandas handle various formats, coerce errors to NaT
            date_obj = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(date_obj):
                 db_date_str = date_obj.strftime('%Y-%m-%d')
            else:
                 logger.warning(f"Could not parse date '{date_str}' for venue '{venue_str}'. Using original.")
                 db_date_str = date_str # Use original string if parsing fails
        except Exception as e:
             logger.error(f"Error parsing date '{date_str}': {e}. Using original.")
             db_date_str = date_str

        # Clean other show fields, providing defaults for missing values
        city = str(row.get('city', '')).strip()
        tour = str(row.get('tour', '')).strip()
        venue = venue_str

        # Determine region using helper function and config map
        region = get_region(city, venue)

        shows.append({
            'original_date_str': date_str, # Keep original for mapping later
            'date': db_date_str,
            'tour': tour if tour else 'Unknown Tour',
            'city': city if city else 'Unknown City',
            'venue': venue,
            'region': region # Add the derived region
        })
        show_keys.add(show_key)

    # Create shows DataFrame, add show_id, reorder columns
    shows_df = pd.DataFrame(shows)
    shows_df.insert(0, 'show_id', range(1, len(shows_df) + 1))

    # Create a mapping from (original_date_str, venue) to show_id for linking setlists
    show_mapping: Dict[Tuple[str, str], int] = {
        (row['original_date_str'], row['venue']): row['show_id'] for _, row in shows_df.iterrows()
    }

    # Drop the temporary original_date_str column before saving
    shows_df_final = shows_df.drop(columns=['original_date_str'])
    # Final column order for shows.csv
    shows_df_final = shows_df_final[['show_id', 'date', 'tour', 'city', 'venue', 'region']]

    logger.info(f"Identified {len(shows_df_final)} unique shows.")

    # --- Prepare Song Matching ---
    # Create a map from normalized title to (original_title, song_id)
    title_map: Dict[str, Tuple[str, int]] = {
         normalize_title(row['title']): (row['title'], row['song_id'])
         for _, row in songs_df.iterrows() if pd.notna(row['title']) # Ensure title exists
    }

    # --- Create Setlists Table ---
    setlists: List[Dict] = []
    # Track frequency and original names of songs not found in songs_df
    missing_songs_dict: Dict[str, Dict[str, Set[str] | int]] = {}
    matching_stats: Dict[str, int] = {}     # Track how songs were matched

    # Ensure necessary setlist columns exist
    required_setlist_cols = ['song', 'position']
    for col in required_setlist_cols:
        if col not in setlists_input_df.columns:
             logger.warning(f"Column '{col}' not found in setlists input. Adding default column.")
             setlists_input_df[col] = ""
    # Handle optional notes column
    if 'notes' not in setlists_input_df.columns:
         setlists_input_df['notes'] = ''


    # --- First Pass: Identify missing songs ---
    logger.info("First pass: Identifying songs missing from the initial songs table...")
    processed_setlist_rows = 0
    total_rows = len(setlists_input_df)
    for _, row in setlists_input_df.iterrows():
        processed_setlist_rows += 1
        if processed_setlist_rows % 20000 == 0:
            logger.debug(f"Scanning setlist row {processed_setlist_rows}/{total_rows} for missing songs...")

        song_title = str(row.get('song', '')).strip()
        date_str = str(row.get('date', '')).strip()
        venue_str = str(row.get('venue', '')).strip()

        # Skip rows without essential info
        if not song_title or not date_str or not venue_str:
            continue

        # Try to find the song using the precomputed title_map
        song_id, match_method = find_best_match(song_title, title_map)

        if song_id:
            # Track matching method counts
             matching_stats[match_method] = matching_stats.get(match_method, 0) + 1
        else:
            # Track missing songs and their counts
             normalized_missing = normalize_title(song_title)
             if normalized_missing: # Only track if normalization results in something
                 if normalized_missing not in missing_songs_dict:
                      missing_songs_dict[normalized_missing] = {'original': set(), 'count': 0}
                 missing_songs_dict[normalized_missing]['original'].add(song_title) # Store original variations
                 missing_songs_dict[normalized_missing]['count'] += 1


    # --- Add missing songs (as covers) to the songs DataFrame ---
    updated_songs_df = songs_df.copy() # Work with a copy
    if missing_songs_dict:
        logger.info(f"Found {len(missing_songs_dict)} unique normalized titles missing. Adding them as covers...")
        next_id = updated_songs_df['song_id'].max() + 1 if not updated_songs_df.empty else 1

        # Sort missing songs alphabetically by normalized title for consistent processing
        sorted_missing = sorted(missing_songs_dict.items())

        for normalized_title, data in sorted_missing:
            # Choose one representative original title (e.g., the shortest or first alphabetically)
            representative_title = sorted(list(data['original']))[0].title() # Use title case
            count = data['count']

            # Check again if the normalized title is now in the map (should not happen with current logic, but safe)
            # This check is important: ensures we don't add duplicates if multiple original titles normalize to the same thing
            # and that normalized title wasn't somehow already present (e.g., as a cover initially).
            if normalize_title(representative_title) not in title_map:
                new_song = pd.DataFrame([{
                    'song_id': next_id,
                    'title': representative_title,
                    'album': 'Cover', # Assume missing songs are covers
                    'is_outtake': 0
                }])
                updated_songs_df = pd.concat([updated_songs_df, new_song], ignore_index=True)

                # Update the title_map immediately for the second pass
                # Use the *normalized* version of the representative title as the key
                current_normalized = normalize_title(representative_title)
                title_map[current_normalized] = (representative_title, next_id)

                logger.info(f"  - Added '{representative_title}' (played {count} times) as song_id {next_id}")
                next_id += 1
            else:
                 # This case means the normalized title was already in the map,
                 # possibly added from KNOWN_COVERS_RAW or a different variant earlier in the loop.
                 # We don't need to add it again.
                 logger.debug(f"  - Skipped adding '{representative_title}' again, already mapped to ID {title_map[normalize_title(representative_title)][1]}.")


        logger.info("Finished adding missing songs.")

    # Log matching statistics
    logger.info("Song matching statistics:")
    total_matched_initially = sum(matching_stats.values())
    total_entries_processed = len(setlists_input_df[setlists_input_df['song'].notna() & (setlists_input_df['song'] != '')])
    logger.info(f"  Total setlist entries with song titles: {total_entries_processed}")
    logger.info(f"  Entries matched in first pass: {total_matched_initially}")
    for method, count in sorted(matching_stats.items()):
        logger.info(f"  - {method}: {count} matches")
    logger.info(f"  Unique normalized titles initially missing: {len(missing_songs_dict)}")


    # --- Second Pass: Create final setlist entries ---
    logger.info("Second pass: Creating final setlist entries...")
    setlist_entry_id_counter = 1
    processed_count = 0
    skipped_rows = 0

    for _, row in setlists_input_df.iterrows():
        processed_count += 1
        if processed_count % 20000 == 0: # Progress indicator
            logger.debug(f"  Processing setlist row {processed_count}/{total_rows} for final entries...")

        # Get show_id using the original date string and venue
        date_str = str(row.get('date', '')).strip()
        venue_str = str(row.get('venue', '')).strip()
        show_key = (date_str, venue_str)
        show_id = show_mapping.get(show_key)

        # Get song title and find its ID (should exist now)
        song_title = str(row.get('song', '')).strip()
        song_id, match_method = find_best_match(song_title, title_map) # Match again using the updated map

        # Skip if show_id or song_id couldn't be determined
        if not show_id or not song_id:
            if not show_id:
                 logger.warning(f"Skipping row - Could not find show_id for date='{date_str}', venue='{venue_str}'")
            if not song_id and song_title: # Log only if a song title was actually present but not matched
                 logger.warning(f"Skipping row - Could not find song_id for title='{song_title}' (normalized: '{normalize_title(song_title)}') from show {show_key}")
            skipped_rows += 1
            continue

        # Get position, handle potential errors/missing values
        position_val = row.get('position')
        try:
            position = int(float(position_val)) if pd.notna(position_val) else 0
        except (ValueError, TypeError):
            logger.warning(f"Could not convert position '{position_val}' to int for song '{song_title}' (show_id {show_id}). Defaulting to 0.")
            position = 0 # Default to 0 if conversion fails

        # Get notes, handle missing values
        notes = str(row.get('notes', '')).strip()
        if pd.isna(notes): notes = "" # Ensure empty string, not NaN

        setlists.append({
            'setlist_entry_id': setlist_entry_id_counter,
            'show_id': show_id,
            'song_id': song_id,
            'position': position,
            'notes': notes
        })
        setlist_entry_id_counter += 1

    # Create final setlists DataFrame
    setlists_df_final = pd.DataFrame(setlists)
    if not setlists_df_final.empty:
         # Ensure correct column order
        setlists_df_final = setlists_df_final[['setlist_entry_id', 'show_id', 'song_id', 'position', 'notes']]
         # Optional: Sort by show_id and position for better readability in the CSV
         # setlists_df_final = setlists_df_final.sort_values(by=['show_id', 'position'])
    else:
         logger.warning("No setlist entries generated.")

    if skipped_rows > 0:
         logger.warning(f"Skipped {skipped_rows} rows during final setlist creation due to missing show/song IDs.")
    logger.info(f"Generated {len(setlists_df_final)} setlist entries.")

    # Return the finalized DataFrames
    return shows_df_final, setlists_df_final, updated_songs_df

# --- Main Execution Logic ---

def normalize_database_schema():
    """
    Main function to normalize data and create output CSV files.
    """
    logger.info("Starting database schema normalization...")
    logger.info(f"Input path: {INPUT_PATH}")
    logger.info(f"Output path: {OUTPUT_PATH}")

    # --- Load Input Data ---
    songs_sessions_file = INPUT_PATH / SONGS_SESSIONS_FILENAME
    setlists_file = INPUT_PATH / SETLISTS_FILENAME

    if not songs_sessions_file.exists():
        logger.error(f"Songs/Sessions file not found: {songs_sessions_file}")
        return
    if not setlists_file.exists():
        logger.error(f"Setlists file not found: {setlists_file}")
        return

    logger.info(f"Loading {SONGS_SESSIONS_FILENAME}...")
    try:
        # Use utf-8-sig to handle potential BOM (Byte Order Mark)
        songs_sessions_df = pd.read_csv(songs_sessions_file, encoding='utf-8-sig')
        # Standardize column names (lowercase, strip whitespace, replace space with underscore)
        songs_sessions_df.columns = [c.strip().lower().replace(' ', '_') for c in songs_sessions_df.columns]
        # Rename specific columns based on expected variations
        songs_sessions_df.rename(columns={
            'song_title': 'song',
            'album_or_outtake': 'type'
            # Add other potential renames if column names vary
        }, inplace=True)
        logger.info(f"Loaded {len(songs_sessions_df)} rows from songs/sessions file.")
    except Exception as e:
        logger.exception(f"Error loading songs/sessions file: {e}")
        return

    logger.info(f"Loading {SETLISTS_FILENAME}...")
    try:
        setlists_input_df = pd.read_csv(setlists_file, encoding='utf-8-sig', low_memory=False) # low_memory=False can help with mixed types
        # Standardize column names
        setlists_input_df.columns = [c.strip().lower().replace(' ', '_') for c in setlists_input_df.columns]
         # Rename specific columns based on expected variations
        setlists_input_df.rename(columns={
            'comment': 'notes', # Map 'comment' to 'notes' if it exists
             # Add other potential renames
        }, inplace=True)

        # Ensure 'notes' column exists, even if empty
        if 'notes' not in setlists_input_df.columns:
             setlists_input_df['notes'] = ''
        # Ensure 'position' exists, fill potential NaNs temporarily
        if 'position' not in setlists_input_df.columns:
             setlists_input_df['position'] = pd.NA # Use pd.NA for missing numeric, handled later
        else:
             # Convert position to numeric early, coercing errors, fillna handled in create func
             setlists_input_df['position'] = pd.to_numeric(setlists_input_df['position'], errors='coerce')


        logger.info(f"Loaded {len(setlists_input_df)} rows from setlists file.")
        logger.info("NOTE: Setlist data might be incomplete as per user's note.")

    except Exception as e:
        logger.exception(f"Error loading setlists file: {e}")
        return

    # --- Create Tables ---
    # Create the initial songs table from sessions data
    songs_table = create_songs_table(songs_sessions_df)

    # Create shows and setlists tables, updating songs_table with covers found in setlists
    shows_table, setlists_table, updated_songs_table = create_shows_setlists_tables(setlists_input_df, songs_table)

    # Use the final updated songs table
    songs_table_final = updated_songs_table

    # --- Save Output CSVs ---
    logger.info("Saving normalized CSV files...")
    try:
        songs_output_file = OUTPUT_PATH / SONGS_OUTPUT_FILENAME
        shows_output_file = OUTPUT_PATH / SHOWS_OUTPUT_FILENAME
        setlists_output_file = OUTPUT_PATH / SETLISTS_OUTPUT_FILENAME

        # Save with UTF-8 encoding (utf-8-sig includes BOM for Excel compatibility)
        songs_table_final.to_csv(songs_output_file, index=False, encoding='utf-8-sig')
        shows_table.to_csv(shows_output_file, index=False, encoding='utf-8-sig')
        setlists_table.to_csv(setlists_output_file, index=False, encoding='utf-8-sig')

        logger.info("Normalized database schema created successfully:")
        logger.info(f"- Songs table ({songs_output_file.name}): {len(songs_table_final)} rows")
        logger.info(f"- Shows table ({shows_output_file.name}): {len(shows_table)} rows")
        logger.info(f"- Setlists table ({setlists_output_file.name}): {len(setlists_table)} rows")
        logger.info(f"Files saved to {OUTPUT_PATH}")

    except Exception as e:
        logger.exception(f"Error saving CSV files: {e}")

if __name__ == "__main__":
    normalize_database_schema()