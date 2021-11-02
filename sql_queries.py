import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#config['CLUSTER']
HOST = config['CLUSTER']['HOST']
DB_NAME = config['CLUSTER']['DB_NAME']
DB_USER = config['CLUSTER']['DB_USER']
DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
DB_PORT = config['CLUSTER']['DB_PORT']

#config['IAM_ROLE']
ARN = config['IAM_ROLE']['ARN']

#config['S3']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist          VARCHAR,
    auth            VARCHAR,
    firstname       VARCHAR,
    gender          VARCHAR,
    iteminsession   INTEGER,
    lastname        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    sessionid       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT NOT NULL,
    useragent       VARCHAR,
    userid          INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     VARCHAR,
    artist_longitude    VARCHAR,
    artist_location     VARCHAR(MAX),
    artist_name         VARCHAR(MAX),
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP,
    user_id         INTEGER NOT NULL,
    level           VARCHAR,
    song_id         VARCHAR NOT NULL,
    artist_id       VARCHAR NOT NULL,
    session_id      INTEGER NOT NULL,
    location        VARCHAR(MAX),
    user_agent      VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id     INTEGER PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR,
    year        INTEGER NOT NULL,
    duration    FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   VARCHAR PRIMARY KEY,
    name        TEXT,
    location    VARCHAR,
    lattitude   NUMERIC,
    longitude   NUMERIC);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time  TIMESTAMP PRIMARY KEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {}
credentials 'aws_iam_role={}'
json {} compupdate on region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  timestamp 'epoch' + (e.ts / 1000) * interval '1 second' AS start_time,
        e.userid,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid,
        e.location,
        e.useragent
FROM staging_events e
JOIN songs s ON (e.song = s.title)
WHERE e.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT  userid,
        firstname,
        lastname,
        gender,
        level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT  song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT  artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT  start_time AS start_time,
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(weekday FROM start_time) AS weekday
FROM (SELECT timestamp 'epoch' + (e.ts / 1000) * interval '1 second' AS start_time FROM staging_events e)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
