# config.py
# Central configuration for the Springsteen data workflow

import re
from pathlib import Path

# --- Path Configuration ---
SCRIPT_DIR = Path(__file__).resolve().parent # Assumes config.py is in the same dir as the scripts
BASE_PATH = SCRIPT_DIR.parent
INPUT_PATH = BASE_PATH / "2 - Data Sources"
OUTPUT_PATH = BASE_PATH / "3 - Schema Creation"
SCHEMA_SQL_FILE = SCRIPT_DIR / "schema.sql" # Path to your schema SQL file

# --- Input File Names ---
SONGS_SESSIONS_FILENAME = "Sessions, Outtakes, Songs.csv"
SETLISTS_FILENAME = "Setlists.csv"

# --- Output File Names ---
SONGS_OUTPUT_FILENAME = "songs.csv"
SHOWS_OUTPUT_FILENAME = "shows.csv"
SETLISTS_OUTPUT_FILENAME = "setlists.csv"

# --- Database Configuration (Imported by populate_database.py) ---
# Assumes database_config.py exists and provides these
# from database_config import DB_NAME, get_connection_string, get_postgres_connection_string

# --- Normalization Rules (`normalize_data.py`) ---

# Title Replacements (Abbreviations, Symbols)
TITLE_REPLACEMENTS = {
    "&": "AND", "+": "AND", "@": "AT",
    "W/": "WITH ", "W /": "WITH ", "W/ ": "WITH ",
    "FEAT.": "FEATURING", "FEAT": "FEATURING", "FT.": "FEATURING", "FT": "FEATURING",
    "'N": "AND", "N'": "AND",
    " - ": " ", "-": " ", "_": " ",
    ".": "", ",": "", "'": "", "!": "", "?": "",
    "(": "", ")": "", "[": "", "]": "", "{": "", "}": "",
    ":": "", ";": "", "\"": "", "/": " ", "\\": " "
}

# Regex for removing remaining non-alphanumeric (except spaces) and normalizing whitespace
RE_NON_ALPHANUMERIC = re.compile(r'[^\w\s]')
RE_WHITESPACE = re.compile(r'\s+')

# Known Title Typo Corrections
TITLE_TYPO_CORRECTIONS = {
    "8ND": "82ND", "EIGHTYSECOND": "82ND", "EIGHTY SECOND": "82ND", "EIGHTY 2ND": "82ND",
    "DANCIN": "DANCING", "DANC": "DANCING",
    "ROLLIN": "ROLLING", "RUNNIN": "RUNNING", "TALKIN": "TALKING",
    "WAITIN": "WAITING", "LIVIN": "LIVING", "COMIN": "COMING",
    "PARADISE BY THE C": "PARADISE BY THE SEA", # Specific variations first
    "PARADISE BY C": "PARADISE BY THE SEA",
    "PARADISE BY THE": "PARADISE BY THE SEA",
    "PARADISE BY": "PARADISE BY THE SEA",
    "SANTA CLAUS IS COMIN TO TOWN": "SANTA CLAUS IS COMING TO TOWN" # Specific correction
}

# Known Cover Songs (Used to initialize songs table)
# Titles should be in the desired final *normalized* format if possible for easy checking,
# but the initial list uses common spellings for readability. They will be normalized anyway.
KNOWN_COVERS_RAW = [
    "PARADISE BY THE SEA", "TWIST AND SHOUT", "DEVIL WITH THE BLUE DRESS",
    "GOOD GOLLY MISS MOLLY", "SWEET SOUL MUSIC", "QUARTER TO THREE",
    "JERSEY GIRL", "WHO'LL STOP THE RAIN", "WAR", "LUCILLE", "LIGHT OF DAY",
    "SANTA CLAUS IS COMING TO TOWN", "DETROIT MEDLEY", "MONA", "STAND BY ME",
    "SUMMERTIME BLUES", "TRAPPED", "I FOUGHT THE LAW", "ROCKIN' ALL OVER THE WORLD",
    "LONDON CALLING", "SEVEN NIGHTS TO ROCK",
    "(YOUR LOVE KEEPS LIFTING ME) HIGHER AND HIGHER",
    # Add other common covers here...
    "DIRTY WATER", "VIVA LAS VEGAS", "GREAT BALLS OF FIRE"
]

# --- Region Mapping (`normalize_data.py`) ---
# Maps lowercase city names to region strings
CITY_TO_REGION_MAP = {
    # NJ
    'asbury park': 'NJ, USA', 'red bank': 'NJ, USA', 'east rutherford': 'NJ, USA', 'holmdel': 'NJ, USA',
    'atlantic city': 'NJ, USA', 'newark': 'NJ, USA', 'trenton': 'NJ, USA', 'passaic': 'NJ, USA',
    'lincroft': 'NJ, USA', 'union': 'NJ, USA', 'sayreville': 'NJ, USA', 'metuchen': 'NJ, USA',
    'deal': 'NJ, USA', 'long branch': 'NJ, USA', 'neptune city': 'NJ, USA', 'highlands': 'NJ, USA',
    'linden': 'NJ, USA', 'princeton': 'NJ, USA', 'west long branch': 'NJ, USA', 'south orange': 'NJ, USA',
    'cassville': 'NJ, USA', 'pemberton': 'NJ, USA', 'parsippany': 'NJ, USA', 'hackensack': 'NJ, USA',
    'montclair': 'NJ, USA', 'newtown': 'NJ, USA', 'beach haven': 'NJ, USA', 'upper darby': 'PA, USA', # Note: Upper Darby is PA, often grouped with NJ shows
    'cherry hill': 'NJ, USA', 'e wing': 'NJ, USA', 'cookstown': 'NJ, USA', 'blackwood': 'NJ, USA',
    'hazlet': 'NJ, USA', 'rumson': 'NJ, USA', 'fair haven': 'NJ, USA', 'sea bright': 'NJ, USA',
    'eatontown': 'NJ, USA', 'colts neck': 'NJ, USA', 'freehold': 'NJ, USA',
    # NY
    'new york': 'NY, USA', 'uniondale': 'NY, USA', 'roslyn': 'NY, USA', 'ossining': 'NY, USA',
    'brookville': 'NY, USA', 'westbury': 'NY, USA', 'saratoga springs': 'NY, USA', 'buffalo': 'NY, USA',
    'rochester': 'NY, USA', 'syracuse': 'NY, USA', 'augusta': 'NY, USA', 'albany': 'NY, USA',
    'oswego': 'NY, USA', 'schenectady': 'NY, USA', 'garden city': 'NY, USA', 'ithaca': 'NY, USA',
    'troy': 'NY, USA', 'saint bonaventure': 'NY, USA', 'west point': 'NY, USA', 'utica': 'NY, USA',
    'binghamton': 'NY, USA', 'poughkeepsie': 'NY, USA', 'oneonta': 'NY, USA', 'geneva': 'NY, USA',
    'sag harbor': 'NY, USA', 'darien center': 'NY, USA', 'queens': 'NY, USA', 'brooklyn': 'NY, USA', 'elmont': 'NY, USA',
    # PA
    'philadelphia': 'PA, USA', 'pittsburgh': 'PA, USA', 'bryn mawr': 'PA, USA', 'harrisburg': 'PA, USA',
    'hershey': 'PA, USA', 'west chester': 'PA, USA', 'york': 'PA, USA', 'university park': 'PA, USA',
    'kutztown': 'PA, USA', 'lewisburg': 'PA, USA', 'chester': 'PA, USA', 'collegeville': 'PA, USA',
    'easton': 'PA, USA', 'waynesburg': 'PA, USA', 'meadville': 'PA, USA', 'johnstown': 'PA, USA',
    'latrobe': 'PA, USA', 'lititz': 'PA, USA', 'lancaster': 'PA, USA', 'northampton': 'PA, USA',
    'bexley': 'OH, USA', 'reading': 'PA, USA', # Bexley seems misplaced, likely OH. Reading added for PA.
    # MA/CT
    'boston': 'MA/CT, USA', 'cambridge': 'MA/CT, USA', 'worcester': 'MA/CT, USA', 'mansfield': 'MA/CT, USA',
    'foxborough': 'MA/CT, USA', 'springfield': 'MA/CT, USA', 'amherst': 'MA/CT, USA', 'lowell': 'MA/CT, USA',
    'stockbridge': 'MA/CT, USA', 'beverly': 'MA/CT, USA', 'salem': 'MA/CT, USA', 'west hartford': 'MA/CT, USA',
    'hartford': 'MA/CT, USA', 'new haven': 'MA/CT, USA', 'bridgeport': 'MA/CT, USA', 'waterbury': 'MA/CT, USA',
    'wallingford': 'MA/CT, USA', 'hamden': 'MA/CT, USA', 'shelton': 'MA/CT, USA', 'storrs': 'MA/CT, USA',
    'uncasville': 'MA/CT, USA', 'kingston': 'RI, USA', # Kingston usually RI
    'providence': 'RI, USA', # Providence is RI
    'bristol': 'RI, USA', # Bristol is RI
    # CA
    'los angeles': 'CA, USA', 'west hollywood': 'CA, USA', 'inglewood': 'CA, USA', 'santa monica': 'CA, USA',
    'berkeley': 'CA, USA', 'oakland': 'CA, USA', 'san francisco': 'CA, USA', 'san jose': 'CA, USA',
    'mountain view': 'CA, USA', 'anaheim': 'CA, USA', 'palo alto': 'CA, USA', 'stockton': 'CA, USA',
    'sacramento': 'CA, USA', 'san diego': 'CA, USA', 'pasadena': 'CA, USA', 'santa clara': 'CA, USA',
    'universal city': 'CA, USA', 'beverly hills': 'CA, USA', 'santa barbara': 'CA, USA',
    'university of california santa barbara campus': 'CA, USA',
    # OH
    'cleveland': 'OH, USA', 'richfield': 'OH, USA', 'columbus': 'OH, USA', 'cincinnati': 'OH, USA',
    'dayton': 'OH, USA', 'toledo': 'OH, USA', 'akron': 'OH, USA', 'youngstown': 'OH, USA',
    'kent': 'OH, USA', 'oxford': 'OH, USA', 'university heights': 'OH, USA', 'athens': 'OH, USA',
    'parma': 'OH, USA', 'cuyahoga falls': 'OH, USA',
    # DC/MD
    'washington': 'DC/MD, USA', 'landover': 'DC/MD, USA', 'bethesda': 'DC/MD, USA', 'college park': 'DC/MD, USA',
    'gaithersburg': 'DC/MD, USA', 'baltimore': 'DC/MD, USA', 'owings mills': 'DC/MD, USA',
    'towson': 'DC/MD, USA', 'annapolis': 'DC/MD, USA',
    # VA
    'richmond': 'VA, USA', 'hampton': 'VA, USA', 'norfolk': 'VA, USA', 'virginia beach': 'VA, USA',
    'charlottesville': 'VA, USA', 'hampden sydney': 'VA, USA', 'blacksburg': 'VA, USA',
    'williamsburg': 'VA, USA', 'bristow': 'VA, USA', 'fairfax': 'VA, USA',
    # IL / Near IL
    'chicago': 'IL, USA', 'rosemont': 'IL, USA', 'tinley park': 'IL, USA', 'evanston': 'IL, USA',
    'champaign': 'IL, USA', 'carbondale': 'IL, USA', 'normal': 'IL, USA',
    'west lafayette': 'IN, USA', # Geographically close to IL cluster
    'notre dame': 'IN, USA',
    # MI
    'detroit': 'MI, USA', 'auburn hills': 'MI, USA', 'east lansing': 'MI, USA', 'pontiac': 'MI, USA',
    'saginaw': 'MI, USA', 'kalamazoo': 'MI, USA', 'ann arbor': 'MI, USA', 'grand rapids': 'MI, USA',
    'clarkston': 'MI, USA', 'ypsilanti': 'MI, USA',
    # GA
    'atlanta': 'GA, USA', 'duluth': 'GA, USA',
    # TX
    'houston': 'TX, USA', 'dallas': 'TX, USA', 'austin': 'TX, USA', 'san antonio': 'TX, USA',
    'corpus christi': 'TX, USA', 'the woodlands': 'TX, USA', 'grand prairie': 'TX, USA',
    # AZ
    'phoenix': 'AZ, USA', 'tempe': 'AZ, USA', 'glendale': 'AZ, USA', 'tucson': 'AZ, USA',
    # FL
    'orlando': 'FL, USA', 'tampa': 'FL, USA', 'miami': 'FL, USA', 'sunrise': 'FL, USA',
    'hollywood': 'FL, USA', 'st. petersburg': 'FL, USA', 'lakeland': 'FL, USA',
    'pembroke pines': 'FL, USA', 'fern park': 'FL, USA', 'palm beach': 'FL, USA',
    'tallahassee': 'FL, USA', 'jacksonville': 'FL, USA', 'wellington': 'FL, USA',
    # NC
    'charlotte': 'NC, USA', 'greensboro': 'NC, USA', 'chapel hill': 'NC, USA', 'raleigh': 'NC, USA',
    'durham': 'NC, USA', 'fayetteville': 'NC, USA', 'boone': 'NC, USA',
    # WA
    'seattle': 'WA, USA', 'tacoma': 'WA, USA',
    # MO / Near MO
    'st. louis': 'MO, USA', 'kansas city': 'MO, USA',
    # CO
    'denver': 'CO, USA', 'morrison': 'CO, USA',
    # MN
    'st. paul': 'MN, USA', 'minneapolis': 'MN, USA', 'bloomington': 'MN, USA',
    # Other US
    'new orleans': 'LA, USA', 'baton rouge': 'LA, USA', 'shreveport': 'LA, USA',
    'little rock': 'AR, USA',
    'oklahoma city': 'OK, USA', 'tulsa': 'OK, USA', 'norman': 'OK, USA',
    'omaha': 'NE, USA', 'lincoln': 'NE, USA',
    'ames': 'IA, USA', 'des moines': 'IA, USA', 'iowa city': 'IA, USA',
    'memphis': 'TN, USA', 'nashville': 'TN, USA', 'knoxville': 'TN, USA', 'chattanooga': 'TN, USA', 'murfreesboro': 'TN, USA',
    'louisville': 'KY, USA', 'lexington': 'KY, USA',
    'birmingham': 'AL, USA', 'mobile': 'AL, USA', 'auburn': 'AL, USA', 'starkville': 'MS, USA', 'jackson': 'MS, USA',
    'columbia': 'SC, USA',
    'portland': 'OR, USA', # Overwrites previous 'portland' under MA/CT if applicable
    'salt lake city': 'UT, USA',
    # Canada
    'toronto': 'Canada', 'montreal': 'Canada', 'ottawa': 'Canada', 'vancouver': 'Canada',
    'calgary': 'Canada', 'edmonton': 'Canada', 'hamilton': 'Canada', 'north york': 'Canada',
    'moncton': 'Canada', 'winnipeg': 'Canada',
    # UK
    'london': 'UK', 'manchester': 'UK', 'birmingham': 'UK', 'newcastle upon tyne': 'UK',
    'sheffield': 'UK', 'leeds': 'UK', 'brighton': 'UK', 'milton keynes': 'UK',
    'stafford': 'UK', 'coventry': 'UK', 'pilton': 'UK', 'liverpool': 'UK',
    'edinburgh': 'UK', 'glasgow': 'UK', 'cardiff': 'UK', 'sunderland': 'UK',
    # Ireland
    'dublin': 'Ireland', 'belfast': 'Ireland', 'cork': 'Ireland', 'kilkenny': 'Ireland',
    'limerick': 'Ireland', 'slane': 'Ireland',
    # France
    'paris': 'France', "l'île-saint-denis": 'France', 'la courneuve': 'France', 'saint-denis': 'France',
    'nanterre': 'France', 'saint-étienne': 'France', 'montpellier': 'France', 'lyon': 'France',
    'nice': 'France', 'toulon': 'France', 'carhaix-plouguer': 'France', 'strasbourg': 'France', 'pérols': 'France',
    # Spain
    'barcelona': 'Spain', 'madrid': 'Spain', 'gijón': 'Spain', 'san sebastian': 'Spain',
    'valencia': 'Spain', 'bilbao': 'Spain', 'zaragoza': 'Spain', 'seville': 'Spain',
    'valladolid': 'Spain', 'santiago de compostela': 'Spain', 'granada': 'Spain',
    'santander': 'Spain', 'badalona': 'Spain', 'benidorm': 'Spain',
    'pozuelo de alarcón': 'Spain', 'las palmas de gran canaria': 'Spain',
    # Italy
    'milan': 'Italy', 'rome': 'Italy', 'turin': 'Italy', 'florence': 'Italy', 'naples': 'Italy',
    'verona': 'Italy', 'bologna': 'Italy', 'padua': 'Italy', 'trieste': 'Italy', 'assago': 'Italy',
    'casalecchio di reno': 'Italy', 'udine': 'Italy', 'genoa': 'Italy', 'codroipo': 'Italy',
    'ferrara': 'Italy', 'monza': 'Italy', 'perugia': 'Italy', 'castel morrone': 'Italy', 'sanremo': 'Italy',
    # Germany
    'berlin': 'Germany', 'frankfurt': 'Germany', 'hamburg': 'Germany', 'munich': 'Germany',
    'cologne': 'Germany', 'gelsenkirchen': 'Germany', 'düsseldorf': 'Germany', 'leipzig': 'Germany',
    'mannheim': 'Germany', 'offenbach am main': 'Germany', 'bremen': 'Germany',
    'ludwigshafen am rhein': 'Germany', 'hockenheim': 'Germany', 'dresden': 'Germany', 'hanover': 'Germany', 'mönchengladbach': 'Germany',
    # Sweden
    'stockholm': 'Sweden', 'gothenburg': 'Sweden', 'solna': 'Sweden',
    # Norway
    'oslo': 'Norway', 'bergen': 'Norway', 'trondheim': 'Norway', 'drammen': 'Norway', 'fornebu': 'Norway',
    # Denmark
    'copenhagen': 'Denmark', 'herning': 'Denmark', 'roskilde': 'Denmark', 'gentofte': 'Denmark', 'odense': 'Denmark',
    # Finland
    'helsinki': 'Finland', 'turku': 'Finland', 'tampere': 'Finland',
    # Netherlands
    'amsterdam': 'Netherlands', 'rotterdam': 'Netherlands', 'the hague': 'Netherlands',
    'landgraaf': 'Netherlands', 'nijmegen': 'Netherlands', 'arnhem': 'Netherlands',
    # Belgium
    'brussels': 'Belgium', 'antwerp': 'Belgium', 'werchter': 'Belgium', 'merksem': 'Belgium',
    'sint-denijs-westrem': 'Belgium', 'vorst / forest': 'Belgium', 'laken / laeken': 'Belgium',
    # Switzerland
    'zurich': 'Switzerland', 'basel': 'Switzerland', 'bern': 'Switzerland', 'lancy': 'Switzerland', 'geneva': 'Switzerland',
    # Austria
    'vienna': 'Austria',
    # Portugal
    'lisbon': 'Portugal',
    # Czech Republic
    'prague': 'Czech Republic',
    # Poland
    'warsaw': 'Poland',
    # Japan
    'tokyo': 'Japan', 'osaka': 'Japan', 'kyoto': 'Japan',
    # Australia
    'sydney': 'Australia', 'melbourne': 'Australia', 'brisbane': 'Australia', 'perth': 'Australia',
    'adelaide': 'Australia', 'pokolbin': 'Australia', 'newham': 'Australia',
    # New Zealand
    'auckland': 'New Zealand', 'christchurch': 'New Zealand', 'wellington': 'New Zealand',
    # South America
    'rio de janeiro': 'Brazil', 'são paulo': 'Brazil',
    'buenos aires': 'Argentina', 'mendoza': 'Argentina',
    'santiago': 'Chile',
    # Mexico
    'mexico city': 'Mexico',
    # Africa
    'cape town': 'South Africa', 'johannesburg': 'South Africa',
    'harare': 'Zimbabwe',
    'abidjan': "Côte d'Ivoire",
    # Asia
    'new delhi': 'India',
    # Central America
    'san josé': 'Costa Rica',
    # Europe (Other)
    'athens': 'Greece',
    'budapest': 'Hungary',
}