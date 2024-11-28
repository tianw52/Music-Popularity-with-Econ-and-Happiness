import lyricsgenius
import pandas as pd
#from langdetect import detect
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import time
from pyspark.sql import SparkSession, functions, types, Row
from requests.exceptions import Timeout
import pycld2 as cld2
from langdetect import detect

###
###     This code is to grab the lyrics and language from the Genius API for those data which does not have lyrics before
###     Every data store in data_with_lyrics&lan
###





genius = lyricsgenius.Genius("PUT YOUR OWN GENIUS API")



@functions.udf(returnType=types.StructType([
    types.StructField('lyrics', types.StringType()),
    types.StructField('language', types.StringType())
]))
def detect_songs(songs,artists,max_retries=3,sleep_time=5):
    for attempt in range(max_retries):
        try:
            if songs is None or artists is None:
                return {'lyrics': None, 'language': None}
            song = genius.search_song(title=songs,artist=artists)
            if song is None or not song.lyrics:
                return {'lyrics': None, 'language': None}
            else:
                lyrics = song.lyrics
                try:
                    is_reliable, _, details = cld2.detect(lyrics.encode('utf-8', 'ignore').decode('utf-8', 'ignore'))
                    if is_reliable:
                        language = details[0][1]
                        return {'lyrics': lyrics, 'language': language}
                    else:
                        return {'lyrics': lyrics, 'language': 'no'}
                except Exception:
                    return {'lyrics': lyrics, 'language': detect(lyrics)}
        except Exception as e:
            if '429' in str(e):
                print(f"Rate limit reached for {songs} by {artists}. Waiting...")
                time.sleep(900)  # Wait for 15 minutes
            else:
                print(f"Error for {songs} by {artists}: {str(e)}. Retrying...")
                time.sleep(sleep_time)
    return 'Error'


song_schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('new_artist', types.StringType()),
    ])
    

def main(inputs,output):
    df = spark.read.csv(inputs,sep=',',schema=song_schema,header=True)
    songs = df.withColumn('lyrics_language', detect_songs(df['name'],df['new_artist']))
    songs = songs.withColumn("lyrics", songs["lyrics_language"].getItem("lyrics")).withColumn("language",songs["lyrics_language"].getItem("language")).drop("lyrics_language").cache()
    
    songs.select(songs['track_id'],songs['lyrics'],songs['language']).write.json(output, mode="append")
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Language Detection with CLD2').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
