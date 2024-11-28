from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,col, sum, year, weekofyear, rank, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import Window
import sys


schema = StructType([
    StructField("date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("streams", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),  
    StructField("artist_genres", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("name", StringType(), True)
])

def main(input, output):
    spark = SparkSession.builder.appName("Spotify Data Preprocessing").getOrCreate()
    #df = spark.read.csv("/Users/tian_mac/sfu/lab1/project/charts.csv", header=True, schema=schema)
    df = spark.read.csv(input, header=True, schema=schema)
    countries_to_keep = ["gb", "jp", "ca", "sg", "au", "no", "be", "hk", "us",  # Developed Countries
                        "ar", "th", "br", "hn", "my", "ph", "mx", "bg", "tr", "global"]  # Developing Countries
    df = df.withColumn("date", to_date(col("date"), "yyyy/MM/dd")) # parse the date column
    df = df.withColumn("year", year(col("date"))).withColumn("week", weekofyear(col("date"))) # Extract year and week from the date
    # Filter rows for the date range 2018-2022 and specified countries, order by country, date and position
    df_filtered = df.filter((col("date") >= "2018-01-01") & (col("date") <= "2022-12-31")) \
                    .filter(df.country.isin(countries_to_keep)) \
                    .orderBy("country", "date", "position")
                    

    ## rank top 200 songs of the year per country:
    # Calculate total streams
    df_total = df_filtered.groupBy("country", "year", "track_id", "name", "artists", "duration", "artist_genres", "explicit")\
                        .agg(sum("streams").alias("total_streams"), count("week").alias("weeks_on_chart"))
    # Calculate the average yearly streams
    df_total = df_total.withColumn("average_streams", col("total_streams") / col("weeks_on_chart"))
    # only keep songs that occur more than 3 times throughout the year           
    df_multi_occur = df_total.filter(col("weeks_on_chart") >= 3) # threshold changable
    # remove duplicates, keep the row with highest number of streams only
    window_1 = Window.partitionBy("country", "year", "name", "artists").orderBy(col("total_streams").desc(), col("track_id"))
    df_unique = df_multi_occur.withColumn("duplicate", rank().over(window_1)).filter(col("duplicate") == 1).drop("duplicate")
    # Define a window to rank tracks within each country and year by the aggregated yearly streams
    window_2 = Window.partitionBy("country", "year").orderBy(col("average_streams").desc())
    # Rank each track within its country and year based on yearly streams
    df_ranked = df_unique.withColumn("rank", rank().over(window_2))
    # Filter to get the top 200 tracks per country and year
    df_top200 = df_ranked.filter(col("rank") <= 200)
    df_top200_sorted = df_top200.orderBy("country", "year", "rank")
    #df_top200_sorted.coalesce(1).write.option("header", "true").csv("/Users/tian_mac/sfu/lab1/project/df_top200_sorted.csv")
    df_top200_sorted.coalesce(1).write.option("header", "true").csv(output)


    # optional, not sure if it helps anything
    #   split the dataframe by country and store them in a dictonary
    # country_dfs = {}
    # # Loop through each country and filter df_top200_sorted
    # for country in countries_to_keep:
    #     country_df = df_top200_sorted.filter(col("country") == country)
    #     country_dfs[country] = country_df


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    main(inputs, outputs)


## =========== some check =============
# Group by country and year, then count the number of songs in the top 200 for each
# country_year_counts = df_top200_sorted.groupBy("country", "year").count()
# country_year_counts.orderBy("country", "year").show(100)
# duplicate_songs = df_top200_sorted.groupBy("country", "year", "name", "artists") \
#                                   .count() \
#                                   .filter(col("count") > 1)
# duplicate_songs.show()
# duplicate_details = df_top200_sorted.join(duplicate_songs, on=["country", "year", "name", "artists"], how="inner") \
#                                     .select("country", "year", "name", "artists", "track_id", "average_streams", "rank")
# duplicate_details.show(50)
                                    