-- Springsteen Setlists Database Schema for PostgreSQL
-- Final Version: Includes detailed location, show notes, song count,
-- and full definitions for all stored functions.

-- Drop existing functions and tables if they exist to ensure a clean setup
-- Use CASCADE to handle dependencies automatically
DROP FUNCTION IF EXISTS get_subset_song_stats(VARCHAR, DATE, DATE) CASCADE; -- Drop old name
DROP FUNCTION IF EXISTS get_song_stats(DATE, DATE, VARCHAR[]) CASCADE; -- Drop old signature if exists
DROP FUNCTION IF EXISTS get_stats_for_show_ids(INT[]) CASCADE; -- Drop new function signature if exists
DROP FUNCTION IF EXISTS update_song_rarity_levels() CASCADE;
DROP FUNCTION IF EXISTS update_song_play_counts() CASCADE;
DROP FUNCTION IF EXISTS update_show_song_counts() CASCADE;

DROP TABLE IF EXISTS setlists CASCADE;
DROP TABLE IF EXISTS shows CASCADE;
DROP TABLE IF EXISTS songs CASCADE;

-- Songs table
CREATE TABLE songs (
    song_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL UNIQUE,
    album VARCHAR(255),
    is_outtake BOOLEAN DEFAULT FALSE,
    times_played INTEGER DEFAULT 0,
    rarity_level INTEGER
);
COMMENT ON TABLE songs IS 'Stores information about each unique song.';
COMMENT ON COLUMN songs.title IS 'Normalized title of the song.';
COMMENT ON COLUMN songs.album IS 'Album the song primarily appears on, or "Cover", "Unknown", etc.';
COMMENT ON COLUMN songs.is_outtake IS 'True if the song is primarily known as an outtake based on source data.';
COMMENT ON COLUMN songs.times_played IS 'Total number of times the song appears in the setlists table (global count).';
COMMENT ON COLUMN songs.rarity_level IS 'Calculated logarithmic rarity score (1-100 scale, higher is more common globally). Null if not calculated or never played.';


-- Shows table
CREATE TABLE shows (
    show_id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    tour VARCHAR(255),
    venue VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state_name VARCHAR(100) NULL,
    state_code VARCHAR(10) NULL,
    country_name VARCHAR(100) NULL,
    country_code VARCHAR(2) NULL,
    show_notes TEXT NULL,
    song_count INTEGER DEFAULT 0 NULL,
    CONSTRAINT shows_date_venue_unique UNIQUE (date, venue)
);
COMMENT ON TABLE shows IS 'Stores information about each unique concert/show, including detailed location and song count.';
COMMENT ON COLUMN shows.tour IS 'Raw tour name from source data, or Year if missing.';
COMMENT ON COLUMN shows.state_name IS 'State/Province name (e.g., New Jersey), if applicable.';
COMMENT ON COLUMN shows.state_code IS 'State/Province code (e.g., NJ, CA), if applicable.';
COMMENT ON COLUMN shows.country_name IS 'Country name (e.g., United States).';
COMMENT ON COLUMN shows.country_code IS 'ISO 3166-1 alpha-2 country code (e.g., US, GB).';
COMMENT ON COLUMN shows.show_notes IS 'General notes pertaining to the entire show from setlist.fm.';
COMMENT ON COLUMN shows.song_count IS 'Number of songs recorded in the setlist for this show (updated by function).';


-- Setlists table
CREATE TABLE setlists (
    setlist_entry_id SERIAL PRIMARY KEY,
    show_id INTEGER NOT NULL REFERENCES shows(show_id) ON DELETE CASCADE,
    song_id INTEGER NOT NULL REFERENCES songs(song_id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    notes TEXT,
    CONSTRAINT setlists_show_song_position_unique UNIQUE (show_id, song_id, position)
);
COMMENT ON TABLE setlists IS 'Junction table linking songs to shows, representing the order songs were played.';
COMMENT ON COLUMN setlists.notes IS 'Optional notes about this specific song performance (e.g., acoustic, guest). From setlist.fm song.info.';


-- === Indexes ===
CREATE INDEX idx_setlists_show_id ON setlists(show_id);
CREATE INDEX idx_setlists_song_id ON setlists(song_id);
CREATE INDEX idx_shows_date ON shows(date);
CREATE INDEX idx_shows_tour ON shows(tour);
CREATE INDEX idx_shows_country_code ON shows(country_code);
CREATE INDEX idx_shows_state_code ON shows(state_code);


-- === Stored Functions ===

-- Function to update GLOBAL play counts in the songs table
CREATE OR REPLACE FUNCTION update_song_play_counts()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE 'Updating GLOBAL song play counts...';
    -- Update counts for songs that exist in setlists
    UPDATE songs s
    SET times_played = COALESCE(subquery.count, 0)
    FROM (
        SELECT sl.song_id, COUNT(*) as count
        FROM setlists sl
        GROUP BY sl.song_id
    ) as subquery
    WHERE s.song_id = subquery.song_id;

    -- Ensure songs NOT in setlists have a count of 0
    UPDATE songs s
    SET times_played = 0
    WHERE NOT EXISTS (
        SELECT 1 FROM setlists sl WHERE sl.song_id = s.song_id
    );
    RAISE NOTICE 'Finished updating global play counts.';
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_song_play_counts() IS 'Updates the times_played column in the songs table based on GLOBAL counts from the setlists table.';


-- Function to update GLOBAL rarity levels using LOGARITHMIC scale
CREATE OR REPLACE FUNCTION update_song_rarity_levels()
RETURNS VOID AS $$
DECLARE
    min_log_plays NUMERIC; max_log_plays NUMERIC; log_range NUMERIC; rarities_updated INTEGER := 0;
BEGIN
    RAISE NOTICE 'Updating GLOBAL song rarity levels using LOGARITHMIC scale...';
    SELECT MIN(LN(times_played + 1)), MAX(LN(times_played + 1)) INTO min_log_plays, max_log_plays
    FROM songs WHERE times_played > 0;
    IF min_log_plays IS NULL OR max_log_plays IS NULL THEN
        RAISE NOTICE 'No play counts > 0 found. Skipping logarithmic rarity update.';
        UPDATE songs SET rarity_level = NULL; RETURN;
    END IF;
    log_range := max_log_plays - min_log_plays;
    RAISE NOTICE 'Min Log(Plays+1): %, Max Log(Plays+1): %, Log Range: %', min_log_plays, max_log_plays, log_range;
    UPDATE songs
    SET rarity_level =
        CASE
            WHEN times_played <= 0 THEN NULL
            WHEN log_range = 0 THEN 100 -- Assign max rarity if only one play count value exists
            ELSE CEIL( ( (LN(times_played + 1) - min_log_plays) / log_range ) * 100.0 )
        END;
    GET DIAGNOSTICS rarities_updated = ROW_COUNT;
    RAISE NOTICE 'Updated logarithmic rarity levels for % songs.', rarities_updated;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_song_rarity_levels() IS 'Calculates and updates the GLOBAL rarity_level (1-100 scale) using a MIN-MAX SCALED NATURAL LOGARITHM of (times_played + 1). Higher is more common. NULL if never played.';


-- Function to update the song count for each show
CREATE OR REPLACE FUNCTION update_show_song_counts()
RETURNS VOID AS $$
BEGIN
    RAISE NOTICE 'Updating show song counts...';
    -- Update counts for shows that have entries in setlists
    UPDATE shows sh
    SET song_count = COALESCE(subquery.count, 0)
    FROM (
        SELECT sl.show_id, COUNT(sl.song_id) as count -- Count songs per show
        FROM setlists sl
        -- Ensure position > 0 if placeholder rows with position 0 exist and shouldn't be counted
        -- WHERE sl.position > 0
        GROUP BY sl.show_id
    ) as subquery
    WHERE sh.show_id = subquery.show_id;
    -- Ensure shows NOT in setlists have a count of 0
    UPDATE shows sh
    SET song_count = 0
    WHERE NOT EXISTS (
        SELECT 1 FROM setlists sl WHERE sl.show_id = sh.show_id -- AND sl.position > 0 (if filtering above)
    );
    RAISE NOTICE 'Finished updating show song counts.';
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_show_song_counts() IS 'Updates the song_count column in the shows table based on counts from the setlists table.';


-- Function to calculate subset-specific stats based on Show IDs
CREATE OR REPLACE FUNCTION get_stats_for_show_ids(
    p_show_ids INT[] -- Input: An array of show IDs to include
)
RETURNS TABLE (
    song_title VARCHAR,
    subset_times_played INTEGER,
    subset_log_rarity_level INTEGER
)
AS $$
BEGIN
    -- Input validation
    IF p_show_ids IS NULL OR array_length(p_show_ids, 1) IS NULL OR array_length(p_show_ids, 1) = 0 THEN
        RAISE NOTICE 'Input show ID array is empty or NULL. Returning empty set.';
        RETURN QUERY SELECT NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER WHERE FALSE;
        RETURN;
    END IF;

    RETURN QUERY
    WITH
    SelectedShows AS ( SELECT show_id FROM unnest(p_show_ids) AS show_id ),
    _check_shows AS ( SELECT COUNT(*) as show_count FROM SelectedShows ),
    SubsetSongPlays AS (
        SELECT sg.song_id, sg.title, COUNT(sl.setlist_entry_id)::INTEGER AS times_played
        FROM setlists sl
        JOIN SelectedShows ss ON sl.show_id = ss.show_id
        JOIN songs sg ON sl.song_id = sg.song_id
        CROSS JOIN _check_shows c WHERE c.show_count > 0
        -- WHERE sl.position > 0 -- Optionally exclude position 0 placeholders
        GROUP BY sg.song_id, sg.title
        HAVING COUNT(sl.setlist_entry_id) > 0
    ),
    SubsetMinMaxLogPlays AS ( SELECT MIN(LN(ssp.times_played + 1)) as min_log_plays, MAX(LN(ssp.times_played + 1)) as max_log_plays FROM SubsetSongPlays ssp ),
    LogRange AS ( SELECT mmlp.min_log_plays, mmlp.max_log_plays, CASE WHEN mmlp.max_log_plays IS NULL OR mmlp.min_log_plays IS NULL THEN 0 ELSE mmlp.max_log_plays - mmlp.min_log_plays END AS log_range FROM SubsetMinMaxLogPlays mmlp )
    SELECT
        ssp.title::VARCHAR, ssp.times_played,
        CASE
            WHEN lr.max_log_plays IS NULL THEN NULL::INTEGER
            WHEN lr.log_range = 0 THEN 100::INTEGER
            ELSE CEIL( ( (LN(ssp.times_played + 1) - lr.min_log_plays) / lr.log_range ) * 100.0 )::INTEGER
        END AS subset_log_rarity_level
    FROM SubsetSongPlays ssp CROSS JOIN LogRange lr
    ORDER BY subset_log_rarity_level DESC NULLS LAST, ssp.times_played DESC, ssp.title ASC;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_stats_for_show_ids(INT[]) IS 'Calculates song play counts and logarithmic rarity (1-100, higher=more common) for a specific set of shows provided as an array of show_ids. Returns a table with song_title, subset_times_played, and subset_log_rarity_level.';

