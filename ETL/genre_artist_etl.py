import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# @functions.udf(returnType=types.StringType())
# def clean_string(artist_genres):
#     # Convert entries into an actual list by removing
#     # brackets/apostrophes from string and splitting by comma
#     cleaned_string = artist_genres.strip("[]").replace("'", "").split(",")

#     return cleaned_string

def top_150(dataframe, years, countries):
    song_schema = types.StructType([
        types.StructField('country', types.StringType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('artist_genres', types.StringType()),
        types.StructField('explicit', types.BooleanType()),
        types.StructField('rank', types.IntegerType())
    ])

    top150 = spark.createDataFrame([], schema=song_schema)

    # Add only the top 150 entries (already sorted by ascending rank)
    # by country per year
    for i in range(len(years)):
        for j in range(len(countries)):
            current = dataframe.filter((dataframe["year"] == years[i]) & \
                                (dataframe["country"] == countries[j])) \
                                .limit(150)

            top150 = top150.unionAll(current)

    return top150

# Use rlike instead of contains for multiple words
# https://stackoverflow.com/a/58748095
def add_genre(dataframe, column_name, genre_string):
    return dataframe.withColumn(column_name, \
            functions.when(functions.col("artist_genres") \
            .rlike(genre_string), True).otherwise(False))

def find_genres(dataframe, genres):
    # Check every entry to see if it contains the genre string (e.g. "pop")
    # Create new column of bools to tell us whether the artist fits this genre
    # https://stackoverflow.com/a/67451620
    for i in range(len(genres)):
        dataframe = add_genre(dataframe, genres[i], genres[i])

    with_pop = add_genre(dataframe, "pop", "pop|francoton|arrocha|"
                        "eurovision|thai idol|high vibe")

    with_indie_alt = add_genre(with_pop, "indie/alternative", "indie|alt")

    with_country_folk = add_genre(with_indie_alt, "country/folk", \
                "country|folk|sertanejo|pagode|arabesk|" \
                "kleinkunst|cumbia|forro|opm|cuarteto")

    with_edm = add_genre(with_country_folk, "edm", \
                        "edm|trap|house|disco|synth|electro|piseiro|" \
                        "dancehall|afro|dnb|vocaloid|russelater|previa|" \
                        "pluggnb|basshall|j-division|high vibe")

    with_reggaeton = add_genre(with_edm, "reggaeton", "reggae|rkt|perreo")

    with_funk = add_genre(with_reggaeton, "funk", "funk|piseiro")

    with_rap = add_genre(with_funk, "rap", "rap|afro|drill|zhenskiy rep|" \
                        "hypnosis mic")

    with_hip_hop = add_genre(with_rap, "hip hop", "hip hop|phonk|pluggnb|" \
                            "hypnosis mic|j-division|urbano")

    with_classical = add_genre(with_hip_hop, "classical/orchestra", \
                                "classic|orchestra|hollywood|" \
                                "instrumental")

    with_rock = add_genre(with_classical, "rock", "rock|neo mellow")

    with_broadway = add_genre(with_rock, "broadway", "broadway|musical|show tunes")

    with_mexican = add_genre(with_broadway, "mexican", "mexican|mariachi|ranchera")

    with_sad = add_genre(with_mexican, "sad", "sad|emo|blues|" \
                        "musica triste brasileira")

    with_ballad = add_genre(with_sad, "ballad", "ballad|turkce slow sarkilar")

    with_jazz_soul = add_genre(with_ballad, "jazz/soul", "jazz|soul")

    return with_jazz_soul

def sum_genres(genre):
    # Convert the boolean genres to numbers (True is 1, False is 0), then sum
    # https://stackoverflow.com/a/35493937
    return functions.sum(functions.col(genre).cast("integer")).alias(genre)

def main(inputs, output):
    song_schema = types.StructType([
        types.StructField('country', types.StringType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('track_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('artists', types.StringType()),
        types.StructField('duration', types.IntegerType()),
        types.StructField('artist_genres', types.StringType()),
        types.StructField('explicit', types.BooleanType()),
        types.StructField('total_streams', types.IntegerType()),
        types.StructField('weeks_on_chart', types.IntegerType()),
        types.StructField('average_streams', types.FloatType()),
        types.StructField('rank', types.IntegerType())
    ])

    song_data = spark.read.csv(inputs, header=True, schema=song_schema) \
                [["country", "year", "artist_genres", "explicit", "rank"]]

    # Exclude any entries with no data about genre/explicit
    # Cache as it will be used multiple times
    no_nulls = song_data.filter(song_data["artist_genres"] != "[]") \
                        .dropna(subset="explicit") \
                        .orderBy(["year", "country", "rank"]).cache()

    # Check how many songs are left per country each year
    # Less than 200 remaining, so let's just take the top 150 songs
    # song_count = no_nulls.groupby(["year", "country"]).count()
    # song_count.show(100)

    # Extract all countries and years from the columns
    # https://stackoverflow.com/a/60896261
    countries = no_nulls.select("country").distinct().collect()
    countries = [country[0] for country in countries]

    years = no_nulls.select("year").distinct().collect()
    years = [year[0] for year in years]

    # Extract the top 150 songs from the remaining entries
    top150 = top_150(no_nulls, years, countries)

    # General song genres as found in genre_song_etl.py:
    # Pop, Reggaeton, EDM, R&B, Rap, Rock,
    # Indie/Alternative, Country/Folk, Hip Hop,
    # Sertanejo, Funk
    # Fill in the rest based on entries that don't match any column after
    genres = ["r&b", "rock", "singer-songwriter", "lounge"]

    # Cache only if remaining_genres is uncommented below,
    # as it'll be used twice
    found_genres = find_genres(top150, genres)#.cache()

    # Find entries with genres that don't match any of the above
    # If this genre is similar enough to a more widely-known genre,
    # add it to the find_genres function and add it to the filter
    # remaining_genres = found_genres.filter((found_genres["pop"] == False) & \
    #                             (found_genres["reggaeton"] == False) & \
    #                             (found_genres["r&b"] == False) & \
    #                             (found_genres["rap"] == False) & \
    #                             (found_genres["rock"] == False) & \
    #                             (found_genres["hip hop"] == False) & \
    #                             (found_genres["funk"] == False) & \
    #                             (found_genres["indie/alternative"] == False) & \
    #                             (found_genres["country/folk"] == False) & \
    #                             (found_genres["edm"] == False) & \
    #                             (found_genres["jazz/soul"] == False) & \
    #                             (found_genres["lounge"] == False) & \
    #                             (found_genres["mexican"] == False) & \
    #                             (found_genres["classical/orchestra"] == False) & \
    #                             (found_genres["singer-songwriter"] == False) & \
    #                             (found_genres["broadway"] == False) & \
    #                             (found_genres["sad"] == False))

    # remaining_genres.show()
    # print(remaining_genres.count())

    # If genre not found, group into "other" category
    # ("other" category only has ~10 entries/year or less)
    with_other = found_genres.withColumn("other", functions.when( \
                                    (found_genres["pop"] == False) & \
                                    (found_genres["reggaeton"] == False) & \
                                    (found_genres["r&b"] == False) & \
                                    (found_genres["rap"] == False) & \
                                    (found_genres["rock"] == False) & \
                                    (found_genres["hip hop"] == False) & \
                                    (found_genres["funk"] == False) & \
                                    (found_genres["indie/alternative"] == False) & \
                                    (found_genres["country/folk"] == False) & \
                                    (found_genres["edm"] == False) & \
                                    (found_genres["jazz/soul"] == False) & \
                                    (found_genres["lounge"] == False) & \
                                    (found_genres["mexican"] == False) & \
                                    (found_genres["classical/orchestra"] == False) & \
                                    (found_genres["singer-songwriter"] == False) & \
                                    (found_genres["broadway"] == False) & \
                                    (found_genres["sad"] == False), True) \
                                    .otherwise(False))

    # For each year/country, find number of times this genre
    # appeared in the top 150 songs (songs can belong to multiple genres)
    summed_genres = with_other.groupBy(["year", "country"]) \
                        .agg(sum_genres("pop"), \
                            sum_genres("reggaeton"), \
                            sum_genres("r&b"), \
                            sum_genres("rap"), \
                            sum_genres("rock"), \
                            sum_genres("hip hop"), \
                            sum_genres("funk"), \
                            sum_genres("indie/alternative"), \
                            sum_genres("country/folk"), \
                            sum_genres("edm"), \
                            sum_genres("jazz/soul"), \
                            sum_genres("lounge"), \
                            sum_genres("mexican"), \
                            sum_genres("classical/orchestra"), \
                            sum_genres("singer-songwriter"), \
                            sum_genres("broadway"), \
                            sum_genres("sad"), \
                            sum_genres("other"), \
                            sum_genres("explicit"))

    # Add column representing number of non-explicit songs to compare
    # We kept top 150 songs, so 150 - explicit = non-explicit
    with_non_explicit = summed_genres.withColumn("non-explicit", \
                                150 - summed_genres["explicit"])

    all_genres = ["pop", "reggaeton", "r&b", "rap", "rock", "hip hop", \
                "funk", "indie/alternative", "country/folk", "edm", \
                "jazz/soul", "lounge", "mexican", "classical/orchestra", \
                "singer-songwriter", "broadway", "sad", "other", \
                "explicit", "non-explicit"]

    # Use melt function to turn the columns of song genre counts into rows
    # corresponding to each year/country combination
    # https://stackoverflow.com/a/41673644
    combined_genres = with_non_explicit.melt(ids=["year", "country"], \
                                        values=all_genres, \
                                        variableColumnName="genre", \
                                        valueColumnName="count")

    combined_genres.write.partitionBy("country") \
                        .parquet(output, mode="overwrite")

# Sample command to run this code:
# $ spark-submit genre_artist_etl.py top200_year.csv genres_by_artist
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Song genres ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)