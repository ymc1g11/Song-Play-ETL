# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int NOT NULL PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int NOT NULL,
    duration float NOT NULL, 
    UNIQUE (title, artist_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level,
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

songplay_table_bulk_insert = ("""
CREATE TEMPORARY TABLE temp_songplays (LIKE songplays)
ON COMMIT DROP;

COPY temp_songplays (
    start_time, 
    user_id, 
    level,
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
FROM %s DELIMITER ',' CSV;

INSERT INTO songplays (
    start_time, 
    user_id, 
    level,
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
SELECT *
FROM temp_songplays 
ON CONFLICT DO NOTHING;

DROP TABLE temp_songplays;
    
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING
RETURNING artist_id;
""")


time_table_insert = ("""
INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
) VALUES (%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT song_id, artist_id FROM songs 
WHERE title=%s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]