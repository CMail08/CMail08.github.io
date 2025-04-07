-- Use a Common Table Expression (CTE) to first select the desired show IDs
WITH TargetShows AS (
    SELECT
        show_id
    FROM
        shows
    WHERE
        -- Criterion 1: Tours after "Working on a Dream"
        -- We'll approximate this by selecting shows on or after Jan 1, 2010
        -- (Adjust this date if the tour ended later)
        date >= '2008-01-01'

        -- Criterion 2: Shows that have more than 20 songs
        -- Assumes song_count column has been populated by update_show_song_counts()
        AND song_count > 20

        -- Criterion 3: Is not part of "Springsteen on Broadway"
        -- Use the exact tour name as stored in your 'shows' table
        AND tour != 'Springsteen on Broadway'
)
-- Step 2: Call the function, passing the aggregated show IDs from the CTE
SELECT
    song_title,             -- Title of the song
    subset_times_played,    -- How many times it was played within these specific shows
    subset_log_rarity_level -- Rarity (1-100) calculated ONLY within these specific shows
FROM
    get_stats_for_show_ids(
        p_show_ids := (SELECT array_agg(show_id) FROM TargetShows) -- Aggregate selected IDs into an array
    );

-- The final result will be ordered by rarity (most common in subset first),
-- then times played, then title, as defined within the function.

