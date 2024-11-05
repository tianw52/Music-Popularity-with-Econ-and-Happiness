
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from transformers import pipeline


sentiment_analyzer = pipeline("sentiment-analysis", model="Akazi/bert-base-multilingual-uncased")


@functions.udf(returnType=types.StructType([
    types.StructField('mood', types.StringType()),
    types.StructField('scores', types.FloatType())
]))
def analyze_mood(lyrics):
    try:
        if lyrics:
            result = sentiment_analyzer(lyrics[:512])
            return {'mood': result[0]['label'],'scores': float(result[0]['score'])}
        else:
            return {'mood': None,'scores':None}
    except Exception as e:
        print(f"Error analyzing mood: {e}")
        return {'mood': None,'scores':None}


song_schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('lyrics', types.StringType()),
        types.StructField('language', types.StringType()),
    ])
    

def main(input,output):
    df = spark.read.json(input,schema=song_schema)
    songs_df = df.withColumn("mood_score", analyze_mood(df["lyrics"]))
    songs = songs_df.withColumn("mood", songs_df["mood_score"].getItem("mood")).withColumn("scores",songs_df["mood_score"].getItem("scores")).drop("mood_score","lyrics").cache()
    songs.write.json(output, mode="append")
    #songs.select(songs['track_id'],songs['lyrics'],songs['language'],songs['scores']).write.json(output, mode="append")

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
