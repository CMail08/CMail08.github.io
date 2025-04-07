-- Springsteen Setlists Database Schema for PostgreSQL

-- Drop existing functions and tables if they exist to ensure a clean setup
DROP FUNCTION IF EXISTS update_song_play_counts() CASCADE;
DROP FUNCTION IF EXISTS update_song_rarity_levels() CASCADE;
-- Removed trigger references as we call functions explicitly

DROP TABLE IF EXISTS setlists CASCADE;
DROP TABLE IF EXISTS shows CASCADE;
DROP TABLE IF EXISTS songs CASCADE;

-- Songs table
-- Stores information about each unique song
CREATE TABLE songs (
    song_id SERIAL PRIMARY KEY,          -- Unique identifier for each song
    title VARCHAR(255) NOT NULL UNIQUE,  -- Song title (made unique to avoid duplicate song entries)
    album VARCHAR(255),                  -- Album the song belongs to (can be NULL for covers/unreleased)
    is_outtake BOOLEAN DEFAULT FALSE,    -- Flag indicating if the song is an outtake (default is false)
    times_played INTEGER DEFAULT 0,      -- Counter for how many times the song has been played (updated by function)
    rarity_level INTEGER                 -- Rarity score (calculated based on play count)
);
COMMENT ON TABLE songs IS 'Stores information about each unique song.';
COMMENT ON COLUMN songs.song_id IS 'Unique serial identifier for the song.';
COMMENT ON COLUMN songs.title IS 'Title of the song.';
COMMENT ON COLUMN songs.album IS 'Album the song primarily appears on, or "Cover", etc.';
COMMENT ON COLUMN songs.is_outtake IS 'True if the song is primarily known as an outtake.';
COMMENT ON COLUMN songs.times_played IS 'Number of times the song appears in the setlists table.';
COMMENT ON COLUMN songs.rarity_level IS 'Calculated rarity score (0-100 scale, higher is more common). Null if not calculated or never played.';


-- Shows table
-- Stores information about each unique concert/show
CREATE TABLE shows (
    show_id SERIAL PRIMARY KEY,          -- Unique identifier for each show
    date DATE NOT NULL,                  -- Date of the show (YYYY-MM-DD)
    tour VARCHAR(255),                   -- Name of the tour
    city VARCHAR(100),                   -- City where the show took place
    venue VARCHAR(255) NOT NULL,         -- Venue where the show took place
    region VARCHAR(100),                 -- Broader geographical region (e.g., State/Country)
    -- Add a unique constraint on date and venue to prevent duplicate show entries
    CONSTRAINT shows_date_venue_unique UNIQUE (date, venue)
);
COMMENT ON TABLE shows IS 'Stores information about each unique concert/show.';
COMMENT ON COLUMN shows.show_id IS 'Unique serial identifier for the show.';
COMMENT ON COLUMN shows.date IS 'Date the show took place.';
COMMENT ON COLUMN shows.tour IS 'The name of the tour the show belonged to.';
COMMENT ON COLUMN shows.city IS 'City where the show was held.';
COMMENT ON COLUMN shows.venue IS 'Specific venue name.';
COMMENT ON COLUMN shows.region IS 'Broader geographical region (e.g., State, Country).';


-- Setlists table
-- Junction table linking songs to shows, representing the setlist order
CREATE TABLE setlists (
    setlist_entry_id SERIAL PRIMARY KEY, -- Unique identifier for each setlist entry
    show_id INTEGER NOT NULL REFERENCES shows(show_id) ON DELETE CASCADE, -- Foreign key to shows table
    song_id INTEGER NOT NULL REFERENCES songs(song_id) ON DELETE CASCADE, -- Foreign key to songs table
    position INTEGER NOT NULL,           -- Position of the song in the setlist (e.g., 1, 2, 3)
    notes TEXT,                          -- Optional notes about the performance (e.g., guest appearance)
    -- Unique constraint to prevent adding the exact same song at the same position in the same show
    CONSTRAINT setlists_show_song_position_unique UNIQUE (show_id, song_id, position)
);
COMMENT ON TABLE setlists IS 'Junction table linking songs to shows, representing the order songs were played.';
COMMENT ON COLUMN setlists.setlist_entry_id IS 'Unique identifier for this specific entry in a setlist.';
COMMENT ON COLUMN setlists.show_id IS 'Reference to the show this entry belongs to.';
COMMENT ON COLUMN setlists.song_id IS 'Reference to the song played.';
COMMENT ON COLUMN setlists.position IS 'Order in which the song was played during the show.';
COMMENT ON COLUMN setlists.notes IS 'Optional notes about this specific performance (e.g., acoustic, guest).';


-- === Indexes ===
-- Create indexes for faster querying on frequently used columns
CREATE INDEX idx_setlists_show_id ON setlists(show_id);
CREATE INDEX idx_setlists_song_id ON setlists(song_id);
CREATE INDEX idx_shows_date ON shows(date);
CREATE INDEX idx_shows_tour ON shows(tour);
CREATE INDEX idx_songs_title ON songs(title); -- Index for title lookups
CREATE INDEX idx_shows_region ON shows(region); -- Index for the region column


-- === Stored Functions ===

-- Function to update the 'times_played' count in the 'songs' table
CREATE OR REPLACE FUNCTION update_song_play_counts()
RETURNS VOID AS $$
DECLARE
    updated_count INTEGER := 0;
BEGIN
    RAISE NOTICE 'Updating song play counts...';

    -- Update counts based on the setlists table
    -- Using COALESCE ensures songs with 0 plays are updated correctly from NULL count
    UPDATE songs s
    SET times_played = COALESCE(subquery.count, 0)
    FROM (
        SELECT song_id, COUNT(*) as count
        FROM setlists
        GROUP BY song_id
    ) as subquery
    WHERE s.song_id = subquery.song_id;

    -- Set times_played to 0 for songs *not* found in the subquery (i.e., not in setlists)
    UPDATE songs s
    SET times_played = 0
    WHERE NOT EXISTS (
        SELECT 1 FROM setlists sl WHERE sl.song_id = s.song_id
    );

    GET DIAGNOSTICS updated_count = ROW_COUNT; -- Get the number of rows affected by the *second* UPDATE
    RAISE NOTICE 'Updated play counts for songs.'; -- Removed count as it's misleading here
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_song_play_counts() IS 'Updates the times_played column in the songs table based on counts from the setlists table.';


-- Function to update the 'rarity_level' based on 'times_played'
CREATE OR REPLACE FUNCTION update_song_rarity_levels()
RETURNS VOID AS $$
DECLARE
    max_plays INTEGER;
    total_songs INTEGER;
    rarities_updated INTEGER := 0;
BEGIN
    RAISE NOTICE 'Updating song rarity levels...';

    -- Get the maximum play count among songs played at least once
    SELECT MAX(times_played) INTO max_plays FROM songs WHERE times_played > 0;

    -- Get the total count of songs played at least once
    SELECT count(*) INTO total_songs FROM songs WHERE times_played > 0;

    -- Avoid division by zero if no songs have plays or max_plays is 0
    IF max_plays IS NULL OR max_plays = 0 OR total_songs = 0 THEN
        RAISE NOTICE 'No play counts found or max play count is 0. Skipping rarity update.';
        -- Set all rarity levels to NULL for consistency
        UPDATE songs SET rarity_level = NULL;
        RETURN;
    END IF;

    -- Update rarity levels based on percentile thresholds (0-100 scale)
    -- Higher score means more common (closer to 100% of max plays)
    UPDATE songs
    SET rarity_level =
        CASE
            WHEN times_played = 0 THEN NULL -- Set rarity to NULL for never played songs
            -- Calculate percentile rank based on times_played relative to max_plays
            -- Using CEIL to round up. Cast to numeric/decimal for accurate division.
            ELSE CEIL((times_played::numeric / max_plays::numeric) * 100.0)
        END;

    GET DIAGNOSTICS rarities_updated = ROW_COUNT;
    RAISE NOTICE 'Updated rarity levels for % songs.', rarities_updated;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_song_rarity_levels() IS 'Calculates and updates the rarity_level for each song based on its play count relative to the maximum play count (0-100 scale, higher is more common). NULL if never played.';

-- Note: Triggers are removed as statistics will be updated via explicit calls in the Python script after bulk loading.