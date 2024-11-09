
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from transformers import pipeline


sentiment_analyzer = pipeline("sentiment-analysis", model="lxyuan/distilbert-base-multilingual-cased-sentiments-student")

@functions.udf(returnType= types.StringType())
def analyze_mood(lyrics):
    try:
        if lyrics:
            result = sentiment_analyzer(lyrics[:512])
            return result[0]['label']
        else:
            return None
    except Exception as e:
        print(f"Error analyzing mood: {e}")
        return None

def main(input,output):
    df = spark.read.parquet(input)
    df1 = df.filter(df['language'].isin('ms','pt','id','zh','zh_Hant','ja'))
    songs_df = df1.withColumn("mood", analyze_mood(df1["lyrics"]))
    songs =songs_df.drop('lyrics')
    songs.write.partitionBy("language").parquet(output, mode="append")
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('find mood').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
