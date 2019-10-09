import os                                               # Used for Operating System related operations
import configparser                                     # Parsing config file containing AWS credentials and other info
from sys import exit                                    # Used to exit safely from application in case of error
from logging import error                               # Log error/info messages to help debug or check informational messages
from pyspark.sql import SparkSession                    # Main entry point for DataFrame and SQL functionality
import pyspark.sql.types as Spark_DT                    # Used to define Schema struct
from datetime import datetime, timedelta                # Used for Date & Time related operations          
# PySpark functions used in below script
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


"""
Purpose:
  - Extracts JSON formatted Songs data stored on AWS S3, performs transformations, and loads transformed on S3
  - Two tables, Songs and Artists are sourced from song metadata files
  - Tables are loaded back on to S3 in Parquet format, overwrites existing tables

Args:
  - spark: SparkSession() instance
  - input_data: Path to Song data Bucket on AWS S3
  - output_data: Path to AWS S3 Bucket to store data warehouse tables
  - song_schema: Schema to read JSON formatted Song data files
"""
def process_song_data(spark, input_data, output_data, song_schema):    
    # Reading song data files into Spark DataFrame
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')          # Using wildcard '*' to indicate recursive read 
    try:
        df = spark.read.json(song_data, schema=song_schema)
    except Exception as e:
        error(f"Error reading Songs data files while processing Songs data: {e}")
        exit()

    # Extracting data from song DataFrame to create songs table
    try:
        songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    except Exception as e:
        error(f"Error creating songs_table DataFrame: {e}")
        exit()
    
    # Writing songs table to AWS S3 in parquet file format, partitioned by year and artist_id
    song_out_path = os.path.join(output_data, 'sparkify_songs_table/')                              # Output path
    try:
        songs_table.write.parquet(song_out_path, mode='overwrite', partitionBy=('year','artist_id'))
    except Exception as e:
        error(f"Error writing Songs table to S3: {e}")
        exit()

    # Extracting data from song DataFrame to create artists table with distinct artist records
    try:
        artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude' \
                                , 'artist_longitude') \
                          .dropDuplicates()                                                 
    except Exception as e:
        error(f"Error creating artists_table DataFrame: {e}")
        exit()
    
    # Writing artists table to AWS S3 in parquet file format
    artist_out_path = os.path.join(output_data, 'sparkify_artist_table/')                        # output path
    try:
        artists_table.write.parquet(artist_out_path, mode='overwrite')
    except Exception as e:
        error(f"Error writing Artists table to S3: {e}")
        exit()


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

