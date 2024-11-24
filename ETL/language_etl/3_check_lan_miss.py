import pandas as pd
from langdetect import detect
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
import pycld2 as cld2

###
###     This code is to recheck the language that missing in with_lyrics
###     Every data store in data_with_lyrics&lan
###



@functions.udf(returnType=types.StringType())
def detect_songs(lyrics):
    #language ='no'
    try:
        is_reliable, _, details = cld2.detect(lyrics.encode('utf-8', 'ignore').decode('utf-8', 'ignore'))
        if is_reliable:
            language = details[0][1]
            return language
    except Exception as e:
        return detect(lyrics)


schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('lyrics', types.StringType()),
        types.StructField('language', types.StringType()),
])
    

def main(inputs,output):
    df = spark.read.json(inputs,schema=schema).cache()
    
    sn1 = df.filter(df['language'] == 'no')
    sn2 = df.filter(df['language'] != 'no')
    sn1 = sn1.drop(sn1['language'])
    s1 = sn1.withColumn('language',detect_songs(sn1['lyrics']))
    #s1.show(10)

    s1.coalesce(1).write.json(output, mode="append")
    sn2.write.json(output, mode="append")
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
