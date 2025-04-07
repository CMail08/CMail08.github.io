WITH RelevantSongs AS (
    -- Select songs from the specified album era
    SELECT
        song_id,
        title,
        album,
        is_outtake
    FROM songs
    WHERE album IN (
        'Greetings From Asbury Park, N.J.',
        'The Wild, the Innocent & the E Street Shuffle',
        'Born to Run',
        'Darkness on the Edge of Town',
        'The River',
        'Nebraska',
        'Born in the U.S.A.'
    )
),
PhillyShows AS (
    -- Get IDs and dates for shows in Philadelphia
    SELECT show_id, date
    FROM shows
    WHERE city = 'Philadelphia'
),
PhillySongPlays AS (
    -- Correlate relevant songs with Philadelphia shows and
    -- rank play dates
    SELECT
        sl.song_id,
        ps.date AS play_date,
        ROW_NUMBER() OVER(PARTITION BY sl.song_id ORDER BY ps.date DESC) as play_rank
    FROM setlists sl
    JOIN PhillyShows ps ON sl.show_id = ps.show_id
    WHERE sl.song_id IN (SELECT song_id FROM RelevantSongs) -- Only include songs from the relevant era
),
LastPlays AS (
    -- Pivot the ranked play dates to get the last, 2nd
    -- last, and 3rd last dates per song
    SELECT
        song_id,
        MAX(CASE WHEN play_rank = 1 THEN play_date END) as last_played_philly,
        MAX(CASE WHEN play_rank = 2 THEN play_date END) as second_last_played_philly,
        MAX(CASE WHEN play_rank = 3 THEN play_date END) as third_last_played_philly
    FROM PhillySongPlays
    GROUP BY song_id
)
-- Final selection joining relevant songs with their
-- Philadelphia play history
SELECT
    rs.album AS "Session",
    rs.is_outtake AS "Outtake",
    rs.title AS "Song Title",
    COALESCE(TO_CHAR(lp.last_played_philly, 'YYYY-MM-DD'), 'Never Played') AS "Last Time Played in Philly (Date)",
    -- Calculate time elapsed since last played in Philly,
    -- only if it has been played
    CASE
        WHEN lp.last_played_philly IS NOT NULL THEN AGE(CURRENT_DATE, lp.last_played_philly)
        ELSE NULL
    END AS "Time Since Last Played",
    -- Display the 2nd and 3rd prior play dates, or 'N/A' if
    -- fewer than 2 or 3 plays
    COALESCE(TO_CHAR(lp.second_last_played_philly, 'YYYY-MM-DD'), 'N/A') AS "2nd Prior Date",
    COALESCE(TO_CHAR(lp.third_last_played_philly, 'YYYY-MM-DD'), 'N/A') AS "3rd Prior Date"
FROM RelevantSongs rs
LEFT JOIN LastPlays lp ON rs.song_id = lp.song_id
ORDER BY
    lp.last_played_philly ASC NULLS FIRST, -- Show songs never played first, then oldest last played
    rs.title ASC; -- Alphabetical order for songs played on the same last date or never played