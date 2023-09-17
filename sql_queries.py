import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Importing credentials and file paths
ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                event_id    BIGINT IDENTITY(0,1) NOT NULL,
                artist      VARCHAR,               
                auth        VARCHAR,               
                firstName   VARCHAR,               
                gender      VARCHAR,               
                itemInSession INTEGER,             
                lastName    VARCHAR,               
                length      FLOAT,               
                level       VARCHAR,               
                location    VARCHAR,               
                method      VARCHAR,               
                page        VARCHAR,               
                registration VARCHAR,              
                sessionId   INTEGER,               
                song        VARCHAR,               
                status      INTEGER,               
                ts          BIGINT,                
                userAgent   VARCHAR,               
                userId      INTEGER)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER           ,
                artist_id           VARCHAR   DISTKEY SORTKEY        ,
                artist_latitude     FLOAT           ,
                artist_longitude    FLOAT           ,
                artist_location     VARCHAR(550)      ,
                artist_name         VARCHAR(200)      ,
                song_id             VARCHAR            ,
                title               VARCHAR(200)      ,
                duration            FLOAT             ,
                year                INTEGER           )
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
                start_time  TIMESTAMP SORTKEY,
                user_id     INTEGER DISTKEY,
                level       VARCHAR,
                song_id     VARCHAR,
                artist_id   VARCHAR,
                session_id  INTEGER,
                location    VARCHAR,
                user_agent  VARCHAR                 )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS 
users (
                user_id     INTEGER PRIMARY KEY NOT NULL DISTKEY,
                first_name  VARCHAR,
                last_name   VARCHAR,
                gender      VARCHAR,
                level       VARCHAR)
""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                artist_id   VARCHAR(50)          PRIMARY KEY NOT NULL ,
                name        VARCHAR(200)         ,
                location    VARCHAR(500)         ,
                latitude    INTEGER           ,
                longitude   INTEGER            )

""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                song_id     VARCHAR(100)             PRIMARY KEY NOT NULL,
                title       VARCHAR(200)            ,
                artist_id   VARCHAR(200)             ,
                year        INTEGER                 ,
                duration    FLOAT              )

""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                start_time  TIMESTAMP               PRIMARY KEY NOT NULL,
                hour        INTEGER                ,
                day         INTEGER                ,
                week        INTEGER                ,
                month       INTEGER                ,
                year        INTEGER                ,
                weekday     INTEGER                )
""")

# FILL STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FILL FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second', 
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events se 
    JOIN staging_songs AS ss 
        ON (se.artist = ss.artist_name)
        WHERE se.page = 'NextSong'; 
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT se.userId AS user_id,
        se.firstName           AS first_name,
        se.lastName            AS last_name,
        se.gender              AS gender,
        se.level               AS level
    FROM staging_events AS se
        WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title,artist_id,year,duration)
    SELECT  DISTINCT ss.song_id  AS song_id,
        ss.title            AS title,
        ss.artist_id        AS artist_id,
        ss.year             AS year,
        ss.duration         AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT  DISTINCT ss.artist_id       AS artist_id,
            ss.artist_name              AS name,
            ss.artist_location          AS location,
            ss.artist_latitude          AS latitude,
            ss.artist_longitude         AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time ( start_time,hour,day,week,month,year,weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' start_time,
        EXTRACT(hour FROM start_time)     AS hour,
        EXTRACT(day FROM start_time)      AS day,
        EXTRACT(week FROM start_time)     AS week,
        EXTRACT(month FROM start_time)    AS month,
        EXTRACT(year FROM start_time)     AS year,
        EXTRACT(week FROM start_time)     AS weekday
    FROM    staging_events se 
    WHERE se.page = 'NextSong';
""")

# QUALITY CHECKS    

# Define column names for staging_events
staging_events_columns = [  
"event_id", "artist", "auth", "firstName", "gender", "itemInSession", "lastName", "length", "level", "location", "method", "page", "registration",  "sessionId", "song", "status", "ts", "userAgent", "userId"]      
# Define column names for staging_songs
staging_songs_columns = ["num_songs", "artist_id", "artist_latitude", "artist_longitude", "artist_location", "artist_name", "song_id", "title", "duration", "year"]

# Data quality check for staging_events
staging_events_check_queries = [
    f"SELECT '{column}' AS column_name, COUNT(*) FROM staging_events WHERE {column} IS NULL GROUP BY column_name;"
    for column in staging_events_columns]

# Data quality check for staging_songs
staging_songs_check_queries = [
    f"SELECT '{column}' AS column_name, COUNT(*) FROM staging_songs WHERE {column} IS NULL GROUP BY column_name;"
    for column in staging_songs_columns]

# Count table length
count_table_songplays = """ SELECT COUNT (*) FROM songplays"""
count_table_songs = """ SELECT COUNT (*) FROM songs"""
count_table_artists = """ SELECT COUNT (*) FROM artists"""
count_table_users = """ SELECT COUNT (*) FROM users"""
count_table_time = """ SELECT COUNT (*) FROM time"""


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_table_length_queries = [count_table_songplays, count_table_songs, count_table_artists, count_table_users, count_table_time]