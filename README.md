# Introduction

## Purpose of this project

Startup company, Sparkify, launched their new streaming app and collected song and user activities in JSON format, which resides in AWS S3. This project is to proivde a database and ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for analytics team to access and query thier data.

# Database schema design & ETL pipeline

## Database schema design

This project applies star schema consisting of following fact and dimension tables

### Fact table

1. songplays - holds log data associated with song plays, i.e. records with page `NextSong`
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension tables

1. users - holds app user data
    - user_id, first_name, last_name, gender, level
2. songs - holds song data
    - song_id, title, artist_id, year, duration
3. artists - holds artist data
    - artist_id, name, location, latitude, longitude
4. time - holds timestamp data in songplays table, the data broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## ETL pipeline

Sparkify has two types of dataset (song and log dataset). This project creates two ETL pipiline for each dataset.

- ETL for Song dataset (`s3://udacity-dend/song_data`)
    - Create dimention tables named **songs** and **artists** from song_data files in S3
- ETL for Log dataset (`s3://udacity-dend/log_data`)
    - Create dimention tables named **time** and **user** as well as fact table, **songplays** from log_data files

# Files and instructions

## Files in this repository

- `create_tables.py` drops and creates tables
- `etl.py` reads and processes files from `song_data` and `log_data` and loads them to tables
- `etl.ipynb` reads and processes a file from `song_data` and `log_data` and loads the data in to tables
- `sql_queries.py` contains all sql queries
- `test.ipynb` displays the first few rows of each table
- `README.md` discusses this project

## Run instructions

1. Run `create_redshift.ipynb` to create Redshift cluster using the AWS python SDK
2. Run `create_tables.py` to create staging tables along with fact and dimention tables
3. Run `etl.py` to process data from JSON files in S3

## Example queries and results for song play analysis

1. A query to obtain the best 5 location of their app being used
    - `%sql SELECT COUNT (songplay_id) AS TOTAL_PLAY, location FROM songplays GROUP BY location ORDER BY TOTAL_PLAY DESC LIMIT 5;`

2. A query to obtain the app usage by subscription level
    - `%sql SELECT COUNT (songplay_id) AS TOTAL_PLAY, level FROM songplays GROUP BY level;`