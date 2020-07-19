# Import packages of interest
import boto3
from configparser import ConfigParser

# Set connections to AWS Services
iam = boto3.client('iam')

# Get configuration details
config = ConfigParser()
config.read('config.cfg')

# Get IAM role
iam_role = iam.get_role(RoleName=config['iam']['role_name'])['Role']['Arn']

log_data_s3_loc, log_jsonpath_s3_loc, song_data_s3_loc = '{} {} {}'.format(*config['s3'].values()).split(' ')

# Queries to drop tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# Queries to create tables schemas
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR , 
auth VARCHAR , 
firstName VARCHAR , 
gender VARCHAR, 
itemInSession INTEGER, 
lastName VARCHAR , 
length REAL, 
level VARCHAR , 
location VARCHAR ,
method VARCHAR ,
page VARCHAR ,
registration BIGINT,
sessionId INTEGER,
song VARCHAR ,
status INTEGER,
ts TIMESTAMP,
userAgent VARCHAR ,
userId INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude REAL,
artist_longitude REAL,
artist_location VARCHAR ,
artist_name VARCHAR ,
song_id VARCHAR, 
title VARCHAR , 
duration REAL,
year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL SORTKEY DISTKEY, 
user_id INTEGER NOT NULL, 
level VARCHAR, 
song_id VARCHAR  NOT NULL, 
artist_id VARCHAR  NOT NULL, 
session_id INTEGER, 
location VARCHAR , 
user_agent VARCHAR 
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id INTEGER NOT NULL SORTKEY PRIMARY KEY, 
first_name VARCHAR , 
last_name VARCHAR , 
gender VARCHAR(2), 
level VARCHAR 
) DISTSTYLE ALL
""")

song_table_create = ("""
create table songs (
song_id VARCHAR DISTKEY SORTKEY PRIMARY KEY, 
title VARCHAR , 
artist_id VARCHAR  NOT NULL, 
year INTEGER, 
duration REAL
)
""")

artist_table_create = ("""
create table artists (
artist_id VARCHAR SORTKEY PRIMARY KEY, 
name VARCHAR , 
location VARCHAR , 
latitude REAL, 
longitude REAL
) DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE time (
start_time TIMESTAMP SORTKEY PRIMARY KEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday VARCHAR(8) NOT NULL
) DISTSTYLE ALL
""")

# Queries to copy data for staged tables
staging_events_copy = ("""
COPY staging_events from {}
IAM_ROLE '{}'
JSON {}
REGION 'us-west-2'
TIMEFORMAT AS 'epochmillisecs'
""").format(log_data_s3_loc, iam_role, log_jsonpath_s3_loc)

staging_songs_copy = ("""
COPY staging_songs from {}
IAM_ROLE '{}'
JSON 'auto'
region 'us-west-2'
""").format(song_data_s3_loc, iam_role)

# Queries to insert data into the star schema data model we have for the music-app
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT(e.ts)  AS start_time, 
        e.userId        AS user_id, 
        e.level         AS level, 
        s.song_id       AS song_id, 
        s.artist_id     AS artist_id, 
        e.sessionId     AS session_id, 
        e.location      AS location, 
        e.userAgent     AS user_agent
FROM public.staging_events e
JOIN public.staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
AND e.page  =  'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT(userId)    AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender,
        level
FROM public.staging_events
WHERE user_id IS NOT NULL
AND page  =  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
FROM public.staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
FROM public.staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  as weekday
FROM public.songplays;
""")

# Create lists so we can reference them within subsequent programs
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
