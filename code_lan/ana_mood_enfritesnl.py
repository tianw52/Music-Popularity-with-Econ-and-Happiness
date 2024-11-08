
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row
from transformers import pipeline


sentiment_analyzer = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

'''
# Define a generator function to yield chunks
def chunk_generator(text, chunk_size=512):
    for i in range(0, len(text), chunk_size):
        yield text[i:i+chunk_size]
'''
def split_into_chunks(lyrics, chunk_size=512):
    return [lyrics[i:i+chunk_size] for i in range(0, len(lyrics), chunk_size)]
'''
@functions.udf(returnType= types.StringType())
def analyze_mood(lyrics):
    try:

        ####
        if lyrics:
            result = sentiment_analyzer(lyrics[:512])
            if result[0]['label'] == '1 star' or result[0]['label'] == '2 stars':
                return 'negative'
            elif result[0]['label'] == '3 stars':
                return 'neutral'
            elif result[0]['label'] == '4 stars' or result[0]['label'] == '5 stars':
                return 'positive'
        else:
            return None
        #####
        if not lyrics:
            return None

        # Split lyrics into 512-character chunks
        #chunk_size = 512
        #chunks = [lyrics[i:i+chunk_size] for i in range(0, len(lyrics), chunk_size)]

        # Variables to store sentiment counts
        positive_count = 0
        neutral_count = 0
        negative_count = 0
        ############
        # Analyze each chunk
        for chunk in chunk_generator(lyrics, chunk_size=512):
            result = sentiment_analyzer(chunk)
            label = result[0]['label']

        # Count the sentiment for each chunk
            if label in ['1 star', '2 stars']:
                negative_count += 1
            elif label == '3 stars':
                neutral_count += 1
            elif label in ['4 stars', '5 stars']:
                positive_count += 1
        #################
          # Split lyrics into chunks and parallelize
        chunks = sc.parallelize(lyrics)
    
        # Analyze sentiment for each chunk
        sentiments = chunks.map(lambda chunk: sentiment_analyzer(chunk)[0]['label'])
    
        # Aggregate results
        positive_count = sentiments.filter(lambda label: label in ['4 stars', '5 stars']).count()
        neutral_count = sentiments.filter(lambda label: label == '3 stars').count()
        negative_count = sentiments.filter(lambda label: label in ['1 star', '2 stars']).count()

        # Determine the overall mood based on the majority sentiment
        if positive_count >= neutral_count and positive_count >= negative_count:
            return 'positive'
        elif negative_count >= neutral_count and negative_count >= positive_count:
            return 'negative'
        else:
            return 'neutral'
    except Exception as e:
        print(f"Error analyzing mood: {e}")
        return None


song_schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('lyrics', types.StringType()),
        types.StructField('language', types.StringType()),
    ])
'''
@functions.udf(returnType=types.StringType())
def analyze_chunk(chunk):
    result = sentiment_analyzer(chunk)
    label = result[0]['label']
    if label in ['4 stars', '5 stars']:
        return 'positive'
    elif label == '3 stars':
        return 'neutral'
    elif label in ['1 star', '2 stars']:
        return 'negative'
    return None

def main(input,output):
    df = spark.read.parquet(input)
    #df1 = spark.read.parquet(input)
    #df1 = df.filter(df['language'].isin('fr', 'en', 'nl', 'it', 'es'))
    
    df1 = df.filter(df['language'] =='es')
    # Register a UDF for splitting lyrics into chunks
    split_udf = functions.udf(split_into_chunks, functions.ArrayType(types.StringType()))

# Add chunks as a new column and explode to create a row for each chunk
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
        functions.when((functions.col("positive_count") >= functions.col("neutral_count")) & (functions.col("positive_count") >= functions.col("negative_count")) , "positive")
         .when((functions.col("negative_count") >= functions.col("neutral_count")) & (functions.col("positive_count") <= functions.col("negative_count")), "negative")
         .otherwise("neutral")
    )
)

    # Show the results
    df_aggregated.select("track_id","language","mood").write.partitionBy("language").parquet(output, mode="append")

'''

    #df1.show()
    songs_df = df1.withColumn("mood", analyze_mood(df1["lyrics"]))
    #print(songs_df.show())
    #songs = songs_df.withColumn("mood", songs_df["mood_score"].getItem("mood")).withColumn("scores",songs_df["mood_score"].getItem("scores")).drop("mood_score","lyrics").cache()
    songs = songs_df.drop("lyrics")
    songs.show()
    #songs.write.partitionBy("language").parquet(output, mode="append")
    #songs.write.json(output, mode="append")
    #songs.select(songs['track_id'],songs['lyrics'],songs['language'],songs['scores']).write.json(output, mode="append")
    '''
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('find mood').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inputs, output)
    #main(output)
