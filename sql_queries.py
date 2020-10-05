import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_events (
		artist VARCHAR,
		auth VARCHAR,
		firstName VARCHAR,
		gender VARCHAR,
		itemInSession INTEGER,
		lastName VARCHAR,
		length FLOAT,
		level VARCHAR,
		location VARCHAR,
		method VARCHAR,
		page VARCHAR,
		registration FLOAT,
		sessionId INTEGER,
		song VARCHAR,
		status INTEGER,
		ts TIMESTAMP,
		userAgent VARCHAR,
		userId INTEGER
	)
""")
# ['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year']
staging_songs_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_songs (
		num_songs INTEGER,
		artist_id VARCHAR,
		artist_latitude FLOAT,
		artist_longitude FLOAT,
		artist_location VARCHAR,
		artist_name VARCHAR,
		song_id VARCHAR,
		title VARCHAR,
		duration FLOAT,
		year SMALLINT
	)
""")

songplay_table_create = ("""
	CREATE TABLE IF NOT EXISTS songplays (
		songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
		start_time TIMESTAMP REFERENCES time NOT NULL,
		user_id VARCHAR REFERENCES users NOT NULL,
		level VARCHAR,
		song_id VARCHAR REFERENCES songs,
		artist_id VARCHAR REFERENCES artists,
		session_id INTEGER,
		location VARCHAR,
		user_agent VARCHAR
	);
""")

user_table_create = ("""
	CREATE TABLE IF NOT EXISTS users (
		user_id VARCHAR PRIMARY KEY,
		first_name VARCHAR,
		last_name VARCHAR,
		gender VARCHAR,
		level VARCHAR
	);
""")

song_table_create = ("""
	CREATE TABLE IF NOT EXISTS songs (
		song_id VARCHAR PRIMARY KEY,
		title VARCHAR,
		artist_id VARCHAR REFERENCES artists NOT NULL,
		year SMALLINT,
		duration FLOAT
	);
""")

artist_table_create = ("""
	CREATE TABLE IF NOT EXISTS artists (
		artist_id VARCHAR PRIMARY KEY,
		name VARCHAR,
		location VARCHAR,
		latitude FLOAT,
		longitude FLOAT
	);
""")

time_table_create = ("""
	CREATE TABLE IF NOT EXISTS time (
		start_time TIMESTAMP PRIMARY KEY,
		hour SMALLINT,
		day SMALLINT,
		week SMALLINT,
		month SMALLINT,
		year SMALLINT,
		weekday SMALLINT
	);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
