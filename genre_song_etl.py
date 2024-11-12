import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(inputs, output):
    song_schema = types.StructType([
        types.StructField('country', types.StringType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('name', types.StringType()),
        types.StructField('artists', types.StringType()),
        types.StructField('rank', types.IntegerType()),
        types.StructField('genre', types.StringType())
    ])

    # Make sure to cache if using unique_genres
    # to check counts of each genre
    song_data = spark.read.csv(inputs, header=True, schema=song_schema)#.cache()

    # Determine total number of songs that fit into each genre to reference
    # unique_genres = song_data.groupBy("genre").count() \
    #                         .orderBy("count", ascending=False)

    # Combine some of the smaller, similar genres together based on above
    # First list contains the 'before' values to replace
    # Second list contains the 'after' values that replace the original values
    combined_genres = song_data \
                        .replace(["Cantopop", "Mandopop", "Christmas", \
                                "Nu-Disco", "Yakousei", "Progressive Rock", \
                                "EDM", "Art Pop", "Art Rock", \
                                "Latin Rap", "Belgian Pop", "Indo-Pop", \
                                "Country Pop", "Synthwave", "Cloud Rap", \
                                "R&B Soul", "Pop Soul", "Electronic forro", \
                                "Dembow", "Contemporary Country", "Cumbia Pop", \
                                "RKT", "Thai Indie", "Japanese Rock", \
                                "Norwegian Pop", "K-Pop", "Folk Pop", \
                                "Pop Folk", "Turkish Pop", "Piseiro", \
                                "Hybrid Trap", "Country", "J-Pop", \
                                "Thai Pop", "Turkish Rap", "Latin Pop", \
                                "Electronic", "Funk Rock", "Funk Rap", \
                                "Trap Funk", "Emo Pop", "Emo Rap", \
                                "Regional Mexican"],
                                ["Pop", "Pop", "Holiday", \
                                "House", "Rock", "Rock", \
                                "Other Electronic", "Pop", "Rock", \
                                "Rap", "Pop", "Pop", \
                                "Country/Folk", "Other Electronic", "Rap", \
                                "Soul", "Soul", "Other Electronic", \
                                "Reggaeton", "Country/Folk", "Pop", \
                                "Reggaeton", "Indie Pop", "Rock", \
                                "Pop", "Pop", "Country/Folk", \
                                "Country/Folk", "Pop", "Other Electronic", \
                                "Trap", "Country/Folk", "Pop", \
                                "Pop", "Rap", "Pop", \
                                "Other Electronic", "Funk", "Funk", \
                                "Funk", "Emo", "Emo", \
                                "Sertanejo"],
                                "genre")

    grouped_genres = combined_genres.groupBy(["year", "country", "genre"]) \
                                    .count().orderBy(["year", "country"]) \

    grouped_genres.write.partitionBy("country") \
                        .parquet(output, mode="overwrite")

# Sample command to run this code:
# $ spark-submit genre_song_etl.py top10_genres.csv genres_by_song
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Song genres ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)