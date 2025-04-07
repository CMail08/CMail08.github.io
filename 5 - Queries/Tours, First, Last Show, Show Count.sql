    SELECT COUNT(*)
    FROM songs
    WHERE album IN (
        'Greetings From Asbury Park, N.J.',
        'The Wild, the Innocent & the E Street Shuffle',
        'Born to Run',
        'Darkness on the Edge of Town',
        'The River',
        'Nebraska',
        'Born in the U.S.A.'
    );