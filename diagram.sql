CREATE TABLE "events" (
  "songplay_id" INTEGER PRIMARY KEY,
  "start_time" TIMESTAMP,
  "user_id" VARCHAR(50),
  "level" VARCHAR(10),
  "song_id" VARCHAR(40),
  "artist_id" VARCHAR(50),
  "session_id" VARCHAR(50),
  "location" VARCHAR(100),
  "user_agent" VARCHAR(255)
);

CREATE TABLE "users" (
  "user_id" INTEGER PRIMARY KEY,
  "first_name" VARCHAR(50),
  "last_name" VARCHAR(80),
  "gender" VARCHAR(10),
  "level" VARCHAR(10)
);

CREATE TABLE "artists" (
  "artist_id" VARCHAR(50) PRIMARY KEY,
  "name" VARCHAR(500),
  "location" VARCHAR(500),
  "latitude" DECIMAL(9),
  "longitude" DECIMAL(9)
);

CREATE TABLE "songs" (
  "song_id" VARCHAR(50) PRIMARY KEY,
  "title" VARCHAR(500),
  "artist_id" VARCHAR(50),
  "year" INTEGER,
  "duration" DECIMAL(9)
);

CREATE TABLE "time" (
  "start_time" TIMESTAMP PRIMARY KEY,
  "hour" SMALLINT,
  "day" SMALLINT,
  "week" SMALLINT,
  "month" SMALLINT,
  "year" SMALLINT,
  "weekday" SMALLINT
);

ALTER TABLE "events" ADD FOREIGN KEY ("user_id") REFERENCES "users" ("user_id");

ALTER TABLE "events" ADD FOREIGN KEY ("song_id") REFERENCES "songs" ("song_id");

ALTER TABLE "events" ADD FOREIGN KEY ("artist_id") REFERENCES "artists" ("artist_id");

ALTER TABLE "time" ADD FOREIGN KEY ("start_time") REFERENCES "events" ("start_time");
