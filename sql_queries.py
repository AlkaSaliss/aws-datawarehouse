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
		song_id VARCHAR REFERENCES songs NOT NULL,
		artist_id VARCHAR REFERENCES artists NOT NULL,
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
	COPY staging_events FROM 's3://udacity-dend/log_data' 
	CREDENTIALS 'aws_iam_role={}'
	GZIP REGION 'us-west-2'
	JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
	COPY staging_songs FROM 's3://udacity-dend/song_data' 
	CREDENTIALS 'aws_iam_role={}'
	GZIP REGION 'us-west-2'
	JSON 'auto';
""").format(config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
	INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
		SELECT ste.ts,
			ste.userId, 
			ste.level, 
			sts.song_id, 
			sts.artist_id, 
			ste.sessionId,
			ste.location,
			ste.userAgent
		FROM staging_events AS ste
			LEFT JOIN staging_songs AS sts ON (ste.artist = sts.artist_name AND ste.song = sts.title)
		WHERE ste.page = 'NextSong'
""")

user_table_insert = ("""
	INSERT INTO users (user_id, first_name , last_name, gender, level)
		SELECT userId, 
			firstName, 
			lastName,
			gender, 
			level 
		FROM staging_events
		ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
	INSERT INTO songs (song_id, title, artist_id, year, duration)
		SELECT song_id,
			title, 
			artist_id, 
			year,
			duration 
		FROM staging_songs
		ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
	INSERT INTO artists (artist_id, name, location, latitude, longitude)
		SELECT artist_id,
			artist_name, 
			artist_location, 
			artist_latitude, 
			artist_longitude 
		FROM staging_songs
		ON CONFLICT (artist_id) DO NOTHING;
""")

time_table_insert = ("""
	INSERT INTO time (start_time, hour, day, week, month, year, weekday)
			SELECT ts,
				EXTRACT(HOUR FROM ts),
				EXTRACT(DAY FROM ts),
				EXTRACT(WEEK FROM ts),
				EXTRACT(MONTH FROM ts),
				EXTRACT(YEAR FROM ts),
				EXTRACT(ISODOW FROM ts)
			FROM staging_events
		ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, time_table_insert, artist_table_insert, song_table_insert, songplay_table_insert]
