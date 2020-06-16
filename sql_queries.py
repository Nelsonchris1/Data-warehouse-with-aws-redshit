import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"


# CREATE TABLES
staging_events_table_create= ("""
        CREATE TABLE IF NOT EXISTS staging_events(
        event_id        INT IDENTITY(0, 1)  NOT NULL    SORTKEY DISTKEY,
        artist_name     VARCHAR,
        auth            VARCHAR,
        firstname      VARCHAR,
        gender          VARCHAR,
        itemInSession   INTEGER,
        lastname       VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR(4),
        page            VARCHAR,
        registrtion     BIGINT,
        session_id       INTEGER,
        song_title      VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP           NOT NULL,
        user_agent       VARCHAR,
        user_id          INTEGER
        )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
        song_id            VARCHAR,
        num_songs          INTEGER,
        title              VARCHAR,
        artist_name        VARCHAR,
        artist_latitude    FLOAT,
        year               INTEGER,
        duration           FLOAT,
        artist_id          VARCHAR,
        artist_longitude   FLOAT,
        artist_location    VARCHAR
);
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
        songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
        start_time           TIMESTAMP,
        user_id              INTEGER,
        level                VARCHAR,
        song_id              VARCHAR,
        artist_id            VARCHAR,
        session_id           INTEGER,
        location             VARCHAR,
        user_agent           VARCHAR
);
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
        user_id     VARCHAR DISTKEY,
        firstName   VARCHAR,
        lastName    VARCHAR,
        gender      VARCHAR,
        level       VARCHAR,
        PRIMARY KEY (user_id)
        );
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR,
        title       VARCHAR,
        artist_id    VARCHAR DISTKEY,
        year        INT,
        duration    DOUBLE PRECISION,
        PRIMARY KEY (song_id)
        )
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR DISTKEY,
        name        VARCHAR,
        location    VARCHAR,
        longitude   DOUBLE PRECISION,
        latitude    DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
        )
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP SORTKEY DISTKEY,
        hour        INT,
        day         INT,
        week        INT,
        month       INT, 
        year        INT,
        weekday     INT,
        PRIMARY KEY (start_time)
        );
""")

# IN THIS SECTION WE'LL USE A DICT AND PASS PARAMETERS QUERY AND MESSAG
# STAGING TABLES

staging_events_query = ("""
        copy staging_events FROM {}
        credentials 'aws_iam_role={}'
        COMPUPDATE OFF region 'us-west-2'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_events_copy = {
    "query": staging_events_query,
    "message": "COPY staging_events"
}

staging_songs_query = ("""
        copy staging_songs FROM {}
        credentials 'aws_iam_role={}'
        COMPUPDATE OFF region 'us-west-2'
        FORMAT AS JSON 'auto' 
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = {
    "query": staging_songs_query,
    "message": "COPY staging_songs "
}

# FINAL TABLES

songplay_table_insert = {
        "query":("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
    to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
        se.user_id as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id artist_id,
        se.session_id as session_id,
        se.location as location,
        se.user_agent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song_title= ss.title AND se.artist_name=ss.artist_name;
    
"""),
    "message": "INSERT fact_songplay"
}              


user_table_insert = {
    "query": ("""
        INSERT INTO users(user_id, firstName, lastName, gender, level)
        SELECT DISTINCT user_id,
                        firstName AS firstname,
                        lastName AS lastname,
                        gender AS gender,
                        level AS level
        FROM staging_events
        WHERE user_id IS NOT NULL;
    """),
    "message": "INSERT users"
}

song_table_insert = {
     "query": ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
   FROM staging_songs
   WHERE song_id IS NOT NULL;
"""),
    "message": "INSERT songs"
}




artist_table_insert = {
    "query": ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude
)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    AND artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
    ;
"""), 
    "message": "INSERT artists"
}


time_table_insert = {
    "query" :("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
        ts,
        EXTRACT (HOUR FROM ts),
        EXTRACT (DAY FROM ts),
        EXTRACT (WEEK FROM ts),
        EXTRACT (MONTH FROM ts),
        EXTRACT (YEAR FROM ts),
        EXTRACT (DOW FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
"""),
    "message": "INSERT time"
}

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, song_table_insert, user_table_insert, artist_table_insert, time_table_insert]
