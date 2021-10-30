import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
CREATE TABLE staging_events()
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs()
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
    user_id     INTEGER NOT NULL,
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
COPY staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay()
VALUES ();
""")

user_table_insert = ("""
INSERT INTO user()
VALUES ();
""")

song_table_insert = ("""
INSERT INTO song()
VALUES ();
""")

artist_table_insert = ("""
INSERT INTO artist()
VALUES ();
""")

time_table_insert = ("""
INSERT INTO time()
VALUES ();
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
