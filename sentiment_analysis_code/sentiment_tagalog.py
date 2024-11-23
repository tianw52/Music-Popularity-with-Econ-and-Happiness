
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from transformers import pipeline, AutoTokenizer


sentiment_analyzer = pipeline("sentiment-analysis", model="dost-asti/RoBERTa-tl-sentiment-analysis")
tokenizer = AutoTokenizer.from_pretrained("dost-asti/RoBERTa-tl-sentiment-analysis")

def split_into_chunks(lyrics, chunk_size=500):
    tokens = tokenizer.tokenize(lyrics)
    chunks = [' '.join(tokens[i:i + chunk_size]) for i in range(0, len(tokens), chunk_size)]
    return [tokenizer.convert_tokens_to_string(chunk.split()) for chunk in chunks]

@functions.udf(returnType= types.StringType())
def analyze_chunk(chunk):
    result = sentiment_analyzer(chunk)
    if result[0]['label'] =='LABEL_0':
        return 'negative'
    elif result[0]['label'] =='LABEL_1':
        return 'positive'
    else:
        return 'neutral'
    
    
def main(input,output):
    df = spark.read.parquet(input)
    df1 = df.filter( (df['language']=='fil') | (df['language'] == 'tl'))
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