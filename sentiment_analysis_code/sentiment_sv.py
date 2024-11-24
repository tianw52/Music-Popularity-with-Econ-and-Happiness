
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
#from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

tokenizer = AutoTokenizer.from_pretrained("KBLab/megatron-bert-large-swedish-cased-165k")
model = AutoModelForSequenceClassification.from_pretrained("KBLab/robust-swedish-sentiment-multiclass")
classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
#sentiment_analyzer = pipeline("sentiment-analysis", model="KBLab/robust-swedish-sentiment-multiclass",return_all_scores=True)



def split_into_chunks(lyrics, chunk_size=512):
    return [lyrics[i:i+chunk_size] for i in range(0, len(lyrics), chunk_size)]

@functions.udf(returnType= types.StringType())
def analyze_chunk(chunk):
    result = classifier(chunk)
    if result[0]['label'] =='NEGATIVE':
        return 'negative'
    elif result[0]['label'] =='POSITIVE':
        return 'positive'
    elif result[0]['label'] =='NEUTRAL':
        return 'neutral'
    return None

    
    
def main(input,output):

    df = spark.read.parquet(input)

    df1 = df.filter(df['language']=='sv')
    '''
    df1 = df1.limit(10)
    songs_df = df1.withColumn("mood", analyze_chunk(df1["lyrics"]))
    #print(songs_df.show())
    #songs = songs_df.withColumn("mood", songs_df["mood_score"].getItem("mood")).withColumn("scores",songs_df["mood_score"].getItem("scores")).drop("mood_score","lyrics").cache()
    songs = songs_df.drop("lyrics")
    songs.show()
    #songs.write
    '''
    
    # Register a UDF for splitting lyrics into chunks
    split_udf = functions.udf(split_into_chunks, functions.ArrayType(types.StringType()))
    df_chunks = df1.withColumn("chunks", functions.explode(split_udf(functions.col("lyrics"))))
    # Apply the sentiment analysis UDF to each chunk
    df_chunk_sentiments = df_chunks.withColumn("chunk_sentiment", analyze_chunk(functions.col("chunks")))


    # Aggregate sentiments per original lyrics row
    df_aggregated = (
    df_chunk_sentiments
    .groupBy("track_id","language")
    .agg(
        functions.sum(functions.when(functions.col("chunk_sentiment") == 'positive', 1).otherwise(0)).alias("positive_count"),
        functions.sum(functions.when(functions.col("chunk_sentiment") == 'neutral', 1).otherwise(0)).alias("neutral_count"),
        functions.sum(functions.when(functions.col("chunk_sentiment") == 'negative', 1).otherwise(0)).alias("negative_count")
    )
    .withColumn(
        "mood",
        functions.when((functions.col("positive_count") > functions.col("neutral_count")) & (functions.col("positive_count") > functions.col("negative_count")) , "positive")
         .when((functions.col("negative_count") > functions.col("neutral_count")) & (functions.col("positive_count") < functions.col("negative_count")), "negative")
         .otherwise("neutral")
    )
)
    # Show the results
    #df_aggregated.show()
    df_aggregated.select("track_id","language","mood").write.partitionBy("language").parquet(output, mode="append")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('find mood').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
