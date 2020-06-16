# Data Warehouse
__________________________________________________________________
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to

### Requierements
1. Python3
2. Jupyter notebook. Download anaconda for this
3. AWS redshift cluster with 4nodes running

> NOTE!! delete cluster once you are done reviewing to avoid Unnecessary charges

## Source Data
The first dataset
> Contain song data with json extention which has been sotored in an aws S3 > >bucket s3://udacity-dend/song_data

The Second dataset
> Contains Log data with json extention which has been stored in an aws s3 bucket  s3://udacity-dend/log_data

## DATA MODEL
Following are the fact and dimension tables made for this project:

Dimension Tables:
<ul>
    <li>users
    <ul>
        <li>columns: user_id, first_name, last_name, gender, level</li>
    </ul>
    </li>
    <li>songs
    <ul>
        <li>columns: song_id, title, artist_id, year, duration</li>
    </ul>
    </li>
    <li>time
    <ul>
        <li>columns: start_time, hour, day, week, month, year, weekday</li>
    </ul>
    </li>
    <li>artists
    <ul>
        <li>columns: artist_id, name, location, lattitude, longitude</li>
    </ul>
    </li>
    <li>songplays
    <ul>
        <li>columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</li>
    </ul>
    </li>
</ul>

## Project Template
<ul>
    <li>***create_table.py*** is where the fact and dimension tables for the star schema is created in Redshift.</li>
    <li>`etl.py` is where data is loaded from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.     </li>
    <li>`sql_queries.py` is where the SQL statements were defined, which will be imported into the two other files above (`create_table, etl.py`).</li>
    <li>`redshift_cluster.py` is where the redshift cluster and iam_role were created and deleted after use</li>
    <li>`test_queries.py` is where we connect to redshift and query our created tables to see some reuslts</li>
</ul>
