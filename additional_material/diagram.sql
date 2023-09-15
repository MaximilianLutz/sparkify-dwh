// Use DBML to define your database structure
// Docs: https://dbml.dbdiagram.io/docs

Table songplays {
                songplay_id INTEGER PK
                start_time  TIMESTAMP
                user_id     VARCHAR
                level       VARCHAR
                song_id     VARCHAR
                artist_id   VARCHAR
                session_id  VARCHAR
                location    VARCHAR
                user_agent  VARCHAR
}

Table users {
                user_id     INTEGER                 PK
                first_name  VARCHAR
                last_name   VARCHAR
                gender      VARCHAR
                level       VARCHAR
}

Table artists {
              artist_id   VARCHAR(50) PK
              name        VARCHAR(200)
              location    VARCHAR(500)
              latitude    DECIMAL(10)
              longitude   DECIMAL(10)
}

Table songs {
                song_id     VARCHAR(100)
                title       VARCHAR(200)
                artist_id   VARCHAR(200)
                year        INTEGER     
                duration    FLOAT     
}

Table time {
                  start_time  TIMESTAMP PK
                  hour        INTEGER
                  day         INTEGER
                  week        INTEGER
                  month       INTEGER
                  year        INTEGER
                  weekday     INTEGER
}
Ref: songplays.user_id > users.user_id 
Ref: songplays.song_id > songs.song_id 
Ref: songplays.artist_id > artists.artist_id
Ref: songplays.start_time - time.start_time 
