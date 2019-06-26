import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(artist VARCHAR,auth VARCHAR,firstName                                                            VARCHAR,gender VARCHAR,iteminSession INT,lastName VARCHAR,length FLOAT,level                                                VARCHAR,location VARCHAR,method VARCHAR,page VARCHAR,registration FLOAT,sessionId                                            VARCHAR,song VARCHAR,status INT,ts BIGINT,userAgent VARCHAR,userid INT)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs INT,artist_id VARCHAR,
                                 artist_latitude FLOAT, artist_longitude FLOAT,artist_location VARCHAR,artist_name                                            VARCHAR,song_id VARCHAR,title VARCHAR,duration FLOAT,year INT)""")

songplay_table_create =      ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id bigint IDENTITY(0,1), start_time timestamp                                  NOT NULL, user_id int NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar                                  NOT NULL,session_id varchar, location varchar, user_agent varchar,CONSTRAINT                                                pk_on_songplay_id PRIMARY KEY(songplay_id))""")

user_table_create =          ("""CREATE TABLE IF NOT EXISTS users(user_id int NOT NULL,first_name varchar,last_name                                          varchar,gender varchar,level varchar,CONSTRAINT pk_on_user_id PRIMARY KEY(user_id))""")

song_table_create =          ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar NOT NULL, title varchar, artist_id                                          varchar NOT NULL,year int,duration float,CONSTRAINT pk_on_song_id PRIMARY KEY(song_id))""")

artist_table_create =        ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar NOT NULL, artist_name varchar,                                          artist_location varchar, artist_latitude float, artist_longitude float,CONSTRAINT                                            pk_on_artist_id PRIMARY KEY(artist_id))""")

time_table_create =          ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp NOT NULL, hour int, day int, week                                 int,month int, year int, weekday int,CONSTRAINT pk_on_start_time PRIMARY KEY(start_time))""")


# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                       iam_role  '{}'
                       FORMAT AS JSON {};""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))


staging_songs_copy = ("""COPY staging_songs FROM {}
                       iam_role  '{}'
                       FORMAT AS JSON 'auto';""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT 
TIMESTAMP 'epoch' + a.ts/1000 *INTERVAL '1 second' AS START_TIME,
a.userid as user_id,
a.level as level,
b.song_id as song_id,
b.artist_id as artist_id,
a.sessionId as session_id,
a.location as location,
a.userAgent as user_agent
FROM
staging_events a,
staging_songs b
where a.song = b.title
and a.artist = b.artist_name
and a.length = b.duration
and a.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level)
SELECT DISTINCT
userid as user_id,
firstName as first_name,
lastName as last_name,
gender as gender,
level as level
FROM staging_events
where userid is not null
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year,duration)
SELECT DISTINCT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs
where song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,artist_name,artist_location,artist_latitude,artist_longitude)
SELECT DISTINCT 
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
where artist_id is not null
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT
start_time,
EXTRACT(HRS FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS HOUR,
EXTRACT(D FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS DAY,
EXTRACT(W FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS WEEK,
EXTRACT(MON FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS MONTH,
EXTRACT(Y FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS YEAR,
EXTRACT(DW FROM TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS WEEKDAY
FROM songplays
where start_time is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,staging_songs_table_create,songplay_table_create,user_table_create,song_table_create,       artist_table_create,time_table_create]
drop_table_queries =   [staging_events_table_drop,staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,time_table_drop]
copy_table_queries =   [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,user_table_insert,song_table_insert, artist_table_insert, time_table_insert]

