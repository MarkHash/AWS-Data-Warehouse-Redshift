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
ARN = config['IAM_ROLE']

#config['S3']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagint_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist          VARCHAR,
    auth            VARCHAR,
    fistName        VARCHAR,
    gender          VARCHAR,
    iteminsession   INTEGER,
    lastname        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    INTEGER,
    sessionid       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              INTEGER,
    useragent       VARCHAR,
    userid          INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     VARCHAR,
    artist_longitude    VARCHAR,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id     IDENTITY(0,1) NOT NULL,
    start_time      TIMESTAMP,
    user_id         INTEGER NOT NULL,
    level           VARCHAR,
    song_id         VARCHAR NOT NULL,
    artist_id       VARCHAR NOT NULL,
    session_id      INTEGER NOT NULL,
    locatioin       VARCHAR,
    user_agent      VARCHAR);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id     INTEGER NOT NULL PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR,
    year        INTEGER NOT NULL,
    duration    FLOAT);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id   VARCHAR PRIMARY KEY,
    name        VARCHAR
    location    VARCHAR
    lattitude   NUMERIC,
    longitude   NUMERIC);
""")

time_table_create = ("""
CREATE TABLE time(
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
credentials 'aws_iam_role= {}'
json {} compupdate on region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs from {}
credentials 'aws_iam_role= {}'
json 'auto compupdate on region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, locatioin, user_agent)

""")

user_table_insert = ("""
INSERT INTO user(user_id, first_name, last_name, gender, level)
SELECT  userid,
        firstname,
        lastname,
        gender,
        level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO song(song_id, title, artist_id, year, duration)
SELECT  song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist(artist_id, name, location, lattitude, longitude)
SELECT  artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT  ts,
        EXTRCT(hour FROM ts)    AS hour,
        EXTRCT(day FROM ts)     AS day,
        EXTRCT(week FROM ts)    AS week,
        EXTRCT(month FROM ts)   AS month,
        EXTRCT(year FROM ts)    AS year,
        CASE WHEN EXTRCT(ISODOW FROM ts) IN (1, 5) THEN true ELSE false END AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
