import os                                               # Used for Operating System related operations
import configparser                                     # Parsing config file containing AWS credentials and other info
from sys import exit                                    # Used to exit safely from application in case of error
from logging import error                               # Log error/info messages to help debug or check informational messages
from pyspark.sql import SparkSession                    # Main entry point for DataFrame and SQL functionality
import pyspark.sql.types as Spark_DT                    # Used to define Schema struct
from datetime import datetime, timedelta                # Used for Date & Time related operations          
# PySpark functions used in below script
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, dayofweek

"""
  - Retrieve AWS credentials from config file using configparser module
  - Set AWS credentials' Environment Variables; one of the places AWS services check for credentials 
"""
try:
    config = configparser.ConfigParser()
    config.read('dl.cfg')
except Exception as e:
    error(f"Error reading config file: {e}")
    exit()

try:
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
except Exception as e:
    error(f"Error setting AWS credential Environment Variables: {e}")
    exit()


"""
Purpose:
  - Function instantiates or finds existing SparkSession() with given properties
    - Config contains Maven coordinates for jars to be included on the driver and executor classpaths
      - The coordinates are in following format - groupId:artifactId:version
      - Listed JAR file provides dependencies for working with Hadoop on AWS Cloud
  - Returns instantiated SparkSession()
"""
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


"""
Purpose:
  - Extracts JSON formatted Log data stored on AWS S3, performs transformations, and loads transformed tables on S3
  - Two tables, Users and Time are sourced from log data files
  - Tables are loaded back on to S3 in Parquet format, overwrites existing tables

Args:
  - spark: SparkSession() instance
  - input_data: Path to Log data Bucket on AWS S3
  - output_data: Path to AWS S3 Bucket to store data warehouse tables
  - log_schema: Schema to read JSON formatted Log data files
"""
def process_log_data(spark, input_data, output_data, log_schema, song_schema):
    # Reading log data files into Spark DataFrame
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')   # Using wildcard '*' to indicate recursive read recursive read
    try:
        df = spark.read.json(log_data, schema=log_schema)
    except Exception as e:
        error(f"Error reading Log data files: {e}")
        exit()
    
    # Filtering Log DataFrame to only keep records with song plays
    # 'NextSong' value for 'pages' indicates the song was being played
    try:
        df = df.filter(df.page == 'NextSong')
    except Exception as e:
        error(f"Error filtering Log DataFrame: {e}")
        exit()

    # Extracting data from Log DataFrame for users table
    # To keep only latest user record, using repartition and orderBy on Log DataFrame followed by dropping dups
    # Sorting descending to keep latest ts row on the top
    # dropDuplicates keeps first record and drops rest of dups
    try:
        users_table = df.repartition(df.userId) \
                        .select('userId','firstName','lastName','gender','level') \
                        .orderBy(df.ts.desc()) \
                        .dropDuplicates(['userId','firstName','lastName','gender'])
    except Exception as e:
        error(f"Error creating users_table DataFrame: {e}")
        exit()
    
    # Writing users table to AWS S3 in parquet file format
    user_out_path = os.path.join(output_data, 'sparkify_user_table/')                    # Output path
    try:
        users_table.write.parquet(user_out_path, mode="overwrite")
    except Exception as e:
        error(f"Error writing Users table to S3: {e}")
        exit()

    # Defining User Defined Function (UDF) to convert log timestamp (seconds since epoch) to \
    # actual Datetime Type timestamp
    get_timestamp = udf(lambda x: datetime(1970, 1, 1) + timedelta(seconds = x/1000), Spark_DT.TimestampType())
    
    # Creating new column 'timestamp' that holds results of get_timestamp udf applied on 'ts' column 
    try:
        df = df.withColumn('timestamp', get_timestamp(df.ts))
    except Exception as e:
        error(f"Error adding 'timestamp' column to Log DataFrame: {e}")
        exit()
    
    # Creating time_table DataFrame by extracting date/time details from timestamp and \
    # storing as multiple table columns
    try:
        time_table = df.select('ts' \
                              , hour('timestamp').alias('hour') \
                              , dayofmonth('timestamp').alias('day') \
                              , weekofyear('timestamp').alias('week') \
                              , month('timestamp').alias('month') \
                              , year('timestamp').alias('year') \
                              , dayofweek('timestamp').alias('weekday'))
    except Exception as e:
        error(f"Error creating time_table DataFrame: {e}")
        exit()
    
    # Writing songs table to AWS S3 in parquet file format, partitioned by year and month
    time_out_path = os.path.join(output_data, 'sparkify_time_table/')                              # output path
    try:
        time_table.write.parquet(time_out_path, mode='overwrite', partitionBy=('year','month'))
    except Exception as e:
        error(f"time table write error: {e}")
        exit()
   
    # Reading song data files into Spark DataFrame for songplays_table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')          # Using wildcard '*' to indicate recursive read 
    try:
        song_df = spark.read.json(song_data, schema=song_schema)
    except Exception as e:
        error(f"Error reading Songs data files while processing Log data: {e}")
        exit()

    # Extracting columns from joined song and log datasets to create songplays table 
    try:
        songplays_table = df.join(song_df, [df.song == song_df.title \
                                           , df.length == song_df.duration \
                                           , df.artist == song_df.artist_name]) \
                            .select(df.ts \
                                  , df.userId \
                                  , df.level \
                                  , song_df.song_id \
                                  , song_df.artist_id \
                                  , df.sessionId \
                                  , df.location \
                                  , df.userAgent \
                                  , year(df.timestamp).alias('year') \
                                  , month(df.timestamp).alias('month'))
    except Exception as e:
        error(f"Error creating Songplay DataFrame: {e}")
        exit()

    # Writing Songplays table to AWS S3 in parquet file format, partitioned by year and month
    songplays_out_path = os.path.join(output_data, 'sparkify_songplays_table/')                     # Output path
    try:
        songplays_table.write.parquet(songplays_out_path, mode='overwrite', partitionBy=('year','month'))
    except Exception as e:
        error(f"Error writing Songplays table to S3: {e}")
        exit()


"""
Purpose:
  - Instantiate SparkSession() by calling create_spark_session function and stop after processing data
  - Define song_schema and log_schema schemas for the Songs and Log files to be read
  - Call process_song_data to extract, transform, and load Songs data into Songs and Artists tables
  - Call process_log_data to extract, transform, and load Users, Time, and Songplays tables
"""
def main():
    # AWS S3 paths for input and output data storage
    input_data = "s3a://udacity-dend/"                                          # To source raw data
    output_data = "s3a://pyspark-s3-etl-udacity-project-4/"                     # To store transformed DWH tables

    # Instantiate SparkSession()
    try:
        spark = create_spark_session()
    except Exception as e:
        error(f"Error creating SparkSession {e}")
        exit()


    # Songs table schema for JSON read
    song_schema = Spark_DT.StructType([Spark_DT.StructField('artist_id'        , Spark_DT.StringType())
                                     , Spark_DT.StructField('artist_latitude'  , Spark_DT.StringType())
                                     , Spark_DT.StructField('artist_location'  , Spark_DT.StringType())
                                     , Spark_DT.StructField('artist_longitude' , Spark_DT.StringType())
                                     , Spark_DT.StructField('artist_name'      , Spark_DT.StringType())
                                     , Spark_DT.StructField('duration'         , Spark_DT.DoubleType())
                                     , Spark_DT.StructField('num_songs'        , Spark_DT.IntegerType())
                                     , Spark_DT.StructField('song_id'          , Spark_DT.StringType())
                                     , Spark_DT.StructField('title'            , Spark_DT.StringType())
                                     , Spark_DT.StructField('year'             , Spark_DT.IntegerType())
                                       ])
    
    # Calling songs data processing function
    try:
        process_song_data(spark, input_data, output_data, song_schema)    
    except Exception as e:
        error(f"Error processing Songs data: {e}")
        exit()

    # Log table schema for JSON read
    log_schema = Spark_DT.StructType([
                    Spark_DT.StructField('artist'       , Spark_DT.StringType())
                  , Spark_DT.StructField('auth'         , Spark_DT.StringType())
                  , Spark_DT.StructField('firstName'    , Spark_DT.StringType())
                  , Spark_DT.StructField('gender'       , Spark_DT.StringType())
                  , Spark_DT.StructField('itemInSession', Spark_DT.IntegerType())
                  , Spark_DT.StructField('lastName'     , Spark_DT.StringType())
                  , Spark_DT.StructField('length'       , Spark_DT.DoubleType())
                  , Spark_DT.StructField('level'        , Spark_DT.StringType())
                  , Spark_DT.StructField('location'     , Spark_DT.StringType())
                  , Spark_DT.StructField('method'       , Spark_DT.StringType())
                  , Spark_DT.StructField('page'         , Spark_DT.StringType())
                  , Spark_DT.StructField('registration' , Spark_DT.DoubleType())
                  , Spark_DT.StructField('sessionId'    , Spark_DT.IntegerType())
                  , Spark_DT.StructField('song'         , Spark_DT.StringType())
                  , Spark_DT.StructField('status'       , Spark_DT.IntegerType())
                  , Spark_DT.StructField('ts'           , Spark_DT.LongType())
                  , Spark_DT.StructField('userAgent'    , Spark_DT.StringType())
                  , Spark_DT.StructField('userId'       , Spark_DT.StringType())
                                        ])

    # Calling log data processing function
    try:
        process_log_data(spark, input_data, output_data, log_schema, song_schema)
    except Exception as e:
        error(f"Error processing Log data: {e}")
        exit()
    
    # Stop SparkSession
    try:
        spark.stop()
    except Exception as e:
        error(f"Error closing SparkSession {e}")
        exit()

"""
    Run above code if the file is labled __main__
      Python internally labels files at runtime to differentiate between imported files and main file
"""
if __name__ == "__main__":
    main()

