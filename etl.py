import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
import pyspark.sql.functions as F 


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Instantiate a spark cluster."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract raw song data and process in spark cluster then write to data lake.
    
    Arguments:
    spark -- the spark session.
    input_data -- s3 bucket end point where our song data resides. 
    output_data -- s3 bucket where our data lake is. 
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 
                            'artist_id', 'year',            
                            'duration')
    
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
    .parquet(output_data + 'songs_table/')
    print('songs_table loaded')

    # extract columns to create artists table   
    artists_table = df.select('artist_id', col('artist_name').alias('name'), \
                               col('artist_location').alias('location'), \
                                col('artist_latitude').alias('lattitude'), \
                                    col('artist_longitude').alias('longitude'))
    
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table/')
    print('artitst_table loaded')

def process_log_data(spark, input_data, output_data):
    """
    Extract raw log data and process in spark cluster then write to data lake.
    
    Arguments:
    spark -- the spark session.
    input_data -- s3 bucket end point where our log data resides. 
    output_data -- s3 bucket where our data lake is. 
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').select('artist', 'auth', 'firstName', \
                                      'gender', 'iteminSession', 'lastName', \
                                    'length','level', 'location','method', \
                                       'page', 'registration', 'sessionId',\
                                 'song', 'status', 'ts', 'userAgent', 'userId' )

    # extract columns for users table    
    users_table = df.select('userid', 'firstName', 
                               'lastName', 'gender', 
                            'level')
    
    users_table = users_table.drop_duplicates(subset=['userId'])
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table/')
    print('users_table loaded')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('date', from_unixtime('timestamp'))
    
    # extract columns to create time table ##### NOT CORRECT FORMAT 
    time_table = df.select(col('timestamp').alias('start_time'),
                       hour('date').alias('hour'),
                       dayofmonth('date').alias('day'),
                       weekofyear('date').alias('week'),
                       month('date').alias('month'),
                       year('date').alias('year'),
                       date_format('date','E').alias('weekday'))
    
    time_table.printSchema()
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
    .parquet(output_data + 'time_table/')
    print('time_table loaded')
    
    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)
    song_df.printSchema()
    song_df = song_df.select('song_id', 'title', 
                            'artist_id', 'year',            
                            'duration', 'artist_name')
    
  
    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = song_df.join(df,(song_df.artist_name ==  df.artist) & (song_df.title == df.song),"inner")
    songplays_table = songplays_df.select(col('itemInsession').alias('songplay_id'), \
                            col('timestamp').alias('start_time'), \
                             col('userId').alias('user_id'), \
                              'level', 'song_id', 'artist_id', \
                              col('sessionId').alias('session_Id'), 'location', \
                              col('userAgent').alias('user_agent'), year('date').alias('year'), \
                               month('date').alias('month')
                         ).dropDuplicates()
    
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
    .parquet(output_data + 'songplays_table/')
    print('song_plays loaded')

def main():
    """Instantiate spark, process data and send to data lake."""
    spark = create_spark_session()
    input_data = "s3a://*******-******/"
    output_data = 's3a://my-***-***-****-13/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
