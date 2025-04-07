import re
import time
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime

# Define output location
OUTPUT_DIR = r"../2 - Data Sources"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "Sessions.csv")

ALBUMS = [
    "Greetings From Asbury Park NJ",
    "Wild Innocent & E Street Shuffle",
    "Born To Run",
    "Darkness On The Edge Of Town",
    "The River",
    "Nebraska",
    "Born In The USA",
    "Tunnel Of Love",
    "Human Touch",
    "Lucky Town",
    "The Ghost Of Tom Joad",
    "The Rising",
    "Devils and Dust",
    "The Seeger Sessions",
    "Magic",
    "Working On A Dream",
    "Wrecking Ball",
    "High Hopes",
    "Western Stars",
    "Letter To You",
    "Only The Strong Survive"
]

def album_to_slug(name: str) -> str:
    """Convert album name to Brucebase URL slug"""
    # Special case for The Seeger Sessions
    if name == "The Seeger Sessions":
        return "the-seeger-sessions"
        
    lower = name.lower()
    lower = re.sub(r"[^\w\s-]", "", lower)
    lower = re.sub(r"\s+", " ", lower).strip()
    slug = lower.replace(" ", "-")
    return slug

def fetch_html_with_retry(url: str) -> str:
    """Fetch HTML with basic retry logic"""
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            if response.ok:
                return response.text
            print(f"HTTP error {response.status_code}, retrying ({attempt+1}/3)...")
        except Exception as e:
            print(f"Request error: {e}, retrying ({attempt+1}/3)...")
        time.sleep(2)
    
    # Final attempt after longer wait
    try:
        response = requests.get(url, timeout=15)
        return response.text if response.ok else f"Failed after all retries: {response.status_code}"
    except Exception as e:
        return f"Failed after all retries: {e}"

def clean_song_title(title):
    """Clean song titles by removing numbers and extra whitespace"""
    # Skip processing if empty
    if not title or not title.strip():
        return ""
        
    # Remove leading numbers with dots (e.g., "1.")
    title = re.sub(r'^\d+\.\s*', '', title)
    
    # Remove numbers with dots (e.g., "1.1.")
    title = re.sub(r'^\d+\.\d+\.\s*', '', title)
    
    # Remove leading numbers with closing parenthesis (e.g., "1)")
    title = re.sub(r'^\d+\)\s*', '', title)
    
    # Remove numbers in parentheses anywhere (e.g., "(1)")
    title = re.sub(r'\(\d+\)', '', title)
    
    # Remove standalone numbers
    title = re.sub(r'^\d+\s*', '', title)
    
    # Remove # symbol if at the beginning
    title = re.sub(r'^#\s*', '', title)
    
    # Remove "and 0" or similar patterns that appear in Devils and Dust
    title = re.sub(r'and \d+\s*$', '', title)
    
    # Clean up any extra spaces
    title = re.sub(r'\s+', ' ', title)
    
    # Standardize title case and special characters
    title = title.strip()
    
    # Standardize quotation marks
    title = title.replace('"', '"').replace('"', '"')
    
    # Common name standardizations
    name_map = {
        "BORN IN THE U.S.A.": "BORN IN THE USA",
        "BORN IN THE U S A": "BORN IN THE USA",
        "BORN IN THE U.S.A": "BORN IN THE USA",
        "BORN IN THE USA.": "BORN IN THE USA",
        "DANCIN IN THE DARK": "DANCING IN THE DARK",
        "DANCING IN THE DARK.": "DANCING IN THE DARK",
        "THE GHOST OF TOM JOAD.": "THE GHOST OF TOM JOAD",
        "4TH OF JULY ASBURY PARK": "4TH OF JULY, ASBURY PARK (SANDY)",
        "4TH OF JULY, ASBURY PARK": "4TH OF JULY, ASBURY PARK (SANDY)",
        "SANDY": "4TH OF JULY, ASBURY PARK (SANDY)",
        "4TH OF JULY": "4TH OF JULY, ASBURY PARK (SANDY)",
    }
    
    # Check for known standardizations
    for old, new in name_map.items():
        if title.upper() == old:
            return new
    
    return title

def extract_songs(html_content, album_name):
    """Extract songs from both the Released section and Additional Recordings section"""
    soup = BeautifulSoup(html_content, "html.parser")
    results = []
    
    # Find main sections
    released_span = soup.find('span', string="Released")
    additional_span = soup.find('span', string="Additional Recordings")
    details_span = soup.find('span', string="Details")
    
    # ============== EXTRACT ALBUM TRACKS ==============
    if released_span:
        print(f"Found <span>Released</span> for {album_name}")
        
        # Find all tables between Released and either Additional Recordings or Details
        end_marker = additional_span if additional_span else details_span
        
        # Get all tables in between
        album_tables = []
        current = released_span
        
        while current and (not end_marker or current != end_marker):
            if current.name == 'table':
                album_tables.append(current)
            current = current.find_next()
        
        if album_tables:
            print(f"Found {len(album_tables)} album tracks table(s)")
            
            # Process each table
            for table_idx, album_table in enumerate(album_tables):
                print(f"Processing album tracks table {table_idx+1}")
                
                # Extract songs from this table
                for row in album_table.find_all('tr'):
                    cells = row.find_all('td')
                    if not cells or len(cells) < 2:
                        continue
                    
                    # Skip header rows
                    if any('Song Title' in cell.get_text() for cell in cells):
                        continue
                    
                    # Get the song title (usually in the second column)
                    song_idx = 1 if len(cells) > 1 else 0
                    song_title = clean_song_title(cells[song_idx].get_text())
                    
                    if song_title and len(song_title) > 1:
                        # Skip dates or timeline entries
                        if not re.match(r'^\w+ \d{1,2}, \d{4}$', song_title) and not re.match(r'^\w+-\w+, \d{4}$', song_title):
                            results.append({
                                "session": album_name,
                                "type": "Album",
                                "song": song_title
                            })
    
    # ============== EXTRACT OUTTAKES ==============
    if additional_span:
        print(f"Found <span>Additional Recordings</span> for {album_name}")
        
        # First, check if there's a tab structure
        current = additional_span
        found_tabs = False
        
        # Look for a yui-navset div or tabs
        while current and (not details_span or current != details_span):
            # Check for tab structure
            if current.name == 'div' and ('yui-navset' in current.get('class', []) or current.select('.yui-navset')):
                found_tabs = True
                print("Found tab structure in Additional Recordings")
                
                # Find the Released tab
                found_release_tab = False
                for tab in current.select('.yui-nav li a em'):
                    tab_text = tab.get_text().strip()
                    # For High Hopes album, skip the "Born In The USA" tab
                    if album_name == "High Hopes" and tab_text == "Born In The USA":
                        print("Skipping 'Born In The USA' tab for High Hopes album")
                        continue
                        
                    if tab_text == "Released":
                        found_release_tab = True
                        print("Found 'Released' tab")
                        
                        # Get the tab index
                        tab_idx = 0
                        for i, t in enumerate(current.select('.yui-nav li a em')):
                            if t.get_text().strip() == "Released":
                                tab_idx = i
                                break
                        
                        # Find the corresponding content div
                        content_divs = current.select('.yui-content > div')
                        if tab_idx < len(content_divs):
                            content_div = content_divs[tab_idx]
                            
                            # Find all tables in this div
                            outtakes_tables = content_div.find_all('table')
                            for table_idx, outtakes_table in enumerate(outtakes_tables):
                                print(f"Processing outtakes table {table_idx+1}")
                                
                                # Extract songs from this table
                                for row in outtakes_table.find_all('tr'):
                                    cells = row.find_all('td')
                                    if not cells:
                                        continue
                                    
                                    # Skip header rows
                                    if any('Song Title' in cell.get_text() for cell in cells):
                                        continue
                                    
                                    # Get the song title (usually first column)
                                    song_idx = 0
                                    if len(cells) > 1:
                                        # Some outtakes tables might have song title in second column
                                        if 'song' in cells[0].get_text().lower() or 'title' in cells[0].get_text().lower():
                                            song_idx = 1
                                    
                                    song_title = clean_song_title(cells[song_idx].get_text())
                                    
                                    if song_title and len(song_title) > 1:
                                        results.append({
                                            "session": album_name,
                                            "type": "Outtake",
                                            "song": song_title
                                        })
                
                # If we didn't find a Release tab, look for tables directly
                if not found_release_tab and album_name == "Devils and Dust":
                    print("No Release tab found for Devils and Dust, searching for tables in Additional Recordings section")
                    # This is a special case for Devils and Dust
                    # Look for tables in the Additional Recordings section
                    current = additional_span
                    while current and (not details_span or current != details_span):
                        if current.name == 'table':
                            print("Found table in Additional Recordings section for Devils and Dust")
                            # Extract songs from this table
                            for row in current.find_all('tr'):
                                cells = row.find_all('td')
                                if not cells:
                                    continue
                                
                                # Skip header rows
                                if any('Song Title' in cell.get_text() for cell in cells):
                                    continue
                                
                                # Get the song title (usually first column)
                                song_title = clean_song_title(cells[0].get_text())
                                
                                if song_title and len(song_title) > 1:
                                    results.append({
                                        "session": album_name,
                                        "type": "Outtake",
                                        "song": song_title
                                    })
                        current = current.find_next()
                
                break
            
            current = current.find_next()
        
        # If no tab structure found, look for a "Released" heading directly
        if not found_tabs:
            current = additional_span
            released_header = None
            
            while current and (not details_span or current != details_span):
                if current.name in ['span', 'div', 'h1', 'h2', 'h3', 'h4', 'p'] and current.get_text().strip() == "Released":
                    released_header = current
                    print("Found 'Released' header after Additional Recordings")
                    break
                current = current.find_next()
            
            if released_header:
                # Find all tables after this Released header
                outtakes_tables = []
                current = released_header
                
                while current and (not details_span or current != details_span):
                    if current.name == 'table':
                        outtakes_tables.append(current)
                    current = current.find_next()
                
                if outtakes_tables:
                    print(f"Found {len(outtakes_tables)} outtakes table(s)")
                    
                    # Process each table
                    for table_idx, outtakes_table in enumerate(outtakes_tables):
                        print(f"Processing outtakes table {table_idx+1}")
                        
                        # Extract songs from this table
                        for row in outtakes_table.find_all('tr'):
                            cells = row.find_all('td')
                            if not cells:
                                continue
                            
                            # Skip header rows
                            if any('Song Title' in cell.get_text() for cell in cells):
                                continue
                            
                            # Get the song title (usually first column)
                            song_title = clean_song_title(cells[0].get_text())
                            
                            if song_title and len(song_title) > 1:
                                results.append({
                                    "session": album_name,
                                    "type": "Outtake",
                                    "song": song_title
                                })
    
    # Print summary
    album_count = len([r for r in results if r["type"] == "Album"])
    outtake_count = len([r for r in results if r["type"] == "Outtake"])
    print(f"Album tracks found: {album_count}")
    print(f"Outtake tracks found: {outtake_count}")
    
    return results

def write_to_excel_csv(all_results, output_file):
    """
    Write song data to a Microsoft Excel-compatible CSV file.
    """
    print(f"Writing {len(all_results)} songs to Excel-compatible CSV: {output_file}")
    
    # Create DataFrame
    df = pd.DataFrame(all_results)
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save as Microsoft Excel-compatible CSV
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Successfully wrote {len(all_results)} rows to {output_file}")
    
    return True

def main():
    print("Starting Springsteen album session data collection...")
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Keep track of all album songs
    all_album_songs = set()
    all_results = []
    seen_songs = set()  # To track duplicates
    
    # Process all albums
    for album in ALBUMS:
        slug = album_to_slug(album)
        url = f"http://brucebase.wikidot.com/stats:{slug}-studio-sessions"
        print(f"\nProcessing: {album} -> {url}")
        
        html_content = fetch_html_with_retry(url)
        if html_content.startswith("Failed"):
            print(f"Error: {html_content}")
            continue
        
        # Extract songs
        song_data = extract_songs(html_content, album)
        
        # Track album songs
        for item in song_data:
            if item["type"] == "Album":
                all_album_songs.add(item["song"])
        
        # Fix Working on a Dream album - "The Night with the Jersey Devil" should be Outtake only
        if album == "Working On A Dream":
            fixed_data = []
            jersey_devil_found = False
            
            for item in song_data:
                # Skip duplicates of The Night with the Jersey Devil if it's an Album track
                if "Night with the Jersey Devil" in item["song"]:
                    if item["type"] == "Outtake":
                        if not jersey_devil_found:
                            fixed_data.append(item)
                            jersey_devil_found = True
                    # Skip if it's an Album track
                else:
                    fixed_data.append(item)
            
            song_data = fixed_data
        
        all_results.extend(song_data)
    
    # Remove duplicates while preserving order
    deduplicated_results = []
    for item in all_results:
        # Create a unique key for each song
        song_key = (item["session"], item["type"], item["song"])
        if song_key not in seen_songs:
            seen_songs.add(song_key)
            deduplicated_results.append(item)
    
    all_results = deduplicated_results
    
    # Mark outtakes that also appear on albums
    for item in all_results:
        if item["type"] == "Outtake" and item["song"] in all_album_songs:
            item["song"] = item["song"] + " *"
    
    # Write to Microsoft Excel-compatible CSV
    write_to_excel_csv(all_results, OUTPUT_CSV)
    
    # Calculate totals
    total_songs = len(all_results)
    album_count = len([r for r in all_results if r["type"] == "Album"])
    outtake_count = len([r for r in all_results if r["type"] == "Outtake"])
    
    print(f"\nSummary:")
    print(f"- Total albums processed: {len(ALBUMS)}")
    print(f"- Total songs found: {total_songs}")
    print(f"- Album tracks: {album_count}")
    print(f"- Outtake tracks: {outtake_count}")
    print(f"\nDone! Springsteen songs data saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()