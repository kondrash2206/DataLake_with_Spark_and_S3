import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc
import  pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Function that processes songs database
    Inputs: spark (Spark Session) - actual Spark session, input_data (string) - a link to S3 bucket with input json files,
            output_data (string) - a link to S3 Bucket where the resulting tables are to saved 
    '''
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.dropDuplicates(subset = ['song_id']).select('song_id','title','artist_id','year','duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet('{}songs/'.format(output_data))
    
    # extract columns to create artists table
    artists_table = df.dropDuplicates(subset = ['artist_id']).select('artist_id',col('artist_name').alias('name'),
                         col('artist_location').alias('location'),
                         col('artist_latitude').alias('latitude'),
                         col('artist_longitude').alias('longitude')) 
    
    # write artists table to parquet files
    artists_table.write.parquet('{}artists/'.format(output_data))
    
def process_log_data(spark, input_data, output_data):
    '''
    Function that processes event database
    Inputs: spark (Spark Session) - actual Spark session, input_data (string) - a link to S3 bucket with input json files,
            output_data (string) - a link to S3 Bucket where the resulting tables are to saved 
    '''

    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/*/*/*.json")
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.sort(desc('ts'))\
                    .dropDuplicates(subset = ['userid'])\
                    .select(col('userid').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender','level')
    
    # write users table to parquet files
    users_table.write.parquet('{}users/'.format(output_data))
    
    # create datetime column from original timestamp column
    df = df.withColumn('date',F.from_unixtime(col('ts')/1000, "yyyy-MM-dd HH:mm:ss"))
    
    # extract columns to create time table
    time_table = df.dropDuplicates(subset = ['ts']).select(col('ts').alias('start_time'), \
                           F.hour(df.date).alias('hour'),\
                           F.dayofmonth(df.date).alias('day'), \
                           F.weekofyear(df.date).alias('week'), \
                           F.month(df.date).alias('month'), \
                           F.year(df.date).alias('year'),\
                           F.dayofweek(df.date).alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet('{}time/'.format(output_data))
    
    
    # read in song data to use for songplays table
    
    song_df = spark.read.parquet('{}songs/*/*/*'.format(output_data))

    artist_df = spark.read.parquet('{}artists/*'.format(output_data))
    
    # extract columns from joined song and log datasets to create songplays table 
    df.createOrReplaceTempView("log_df")
    song_df.createOrReplaceTempView('song')
    artist_df.createOrReplaceTempView('artist')
    time_table.createOrReplaceTempView('time')

    songplays_table = spark.sql('''
    SELECT log_df.ts as start_time,
           log_df.userid as user_id,
           log_df.level,
           song.song_id,
           artist.artist_id,
           log_df.sessionid as session_id,
           log_df.location,
           log_df.useragent as user_agent,
           time.year,
           time.month
    FROM log_df
           LEFT JOIN song  ON song.title = log_df.song
           LEFT JOIN artist ON artist.name = log_df.artist
           LEFT JOIN time ON time.start_time = log_df.ts''')

    songplays_table = songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet('{}songplays/'.format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-spark-parquet/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
