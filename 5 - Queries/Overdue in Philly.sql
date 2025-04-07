WITH FilteredSongs AS (
    -- Select songs that are either outtakes or from 'Born
    -- In The USA' or earlier albums
    SELECT
        s.song_id,
        s.title,
        s.album,
        s.is_outtake
    FROM songs s
    WHERE
        s.is_outtake = TRUE
        OR s.album IN (
            'Born In The USA',
            'Nebraska',
            'The River',
            'Darkness On The Edge Of Town',
            'Born To Run',
            'Wild Innocent & E Street Shuffle',
            'Greetings From Asbury Park NJ'
        ) 
),
PhillyShows AS (
    -- Select shows that occurred in Philadelphia, PA
    SELECT
        sh.show_id,
        sh.date
    FROM shows sh
    WHERE
        sh.city = 'Philadelphia'
        AND sh.state_code = 'PA' -- Specify Pennsylvania to be sure
),
RankedPerformances AS (
    -- Find performances of the filtered songs at the Philly
    -- shows
    -- and rank them by date (most recent = 1) for each song
    SELECT
        fs.song_id,
        fs.title AS song_title,
        fs.album,
        fs.is_outtake,
        ps.date AS show_date,
        ROW_NUMBER() OVER (PARTITION BY fs.song_id ORDER BY ps.date DESC) as rn
    FROM setlists sl
    JOIN FilteredSongs fs ON sl.song_id = fs.song_id
    JOIN PhillyShows ps ON sl.show_id = ps.show_id
)
-- Select the song details and pivot the top 3 most
-- recent dates into separate columns
SELECT
    rp.album,
    rp.is_outtake,
    rp.song_title AS "Song",
    MAX(CASE WHEN rn = 1 THEN rp.show_date END) AS "Show 1 Date", -- Most recent date
    MAX(CASE WHEN rn = 2 THEN rp.show_date END) AS "Show 2 Date", -- Second most recent
    MAX(CASE WHEN rn = 3 THEN rp.show_date END) AS "Show 3 Date"  -- Third most recent
FROM RankedPerformances rp
WHERE rp.rn <= 3 -- Only consider the top 3 performances per song
GROUP BY
    rp.song_id, -- Ensure grouping by unique song ID
    rp.song_title,
    rp.album,
    rp.is_outtake
ORDER BY
    "Show 1 Date" asc;