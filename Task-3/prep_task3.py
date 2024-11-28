from pyspark.sql import SparkSession
from pyspark.sql.functions import col,col, count, row_number
from pyspark.sql.window import Window
import sys

def main(mood_data_path, top200_path, happy_path, output):
    spark = SparkSession.builder.appName("Happiness and Song Mood").getOrCreate()

    #mood_data_path = "/Users/tian_mac/sfu/lab1/cmpt732-spotify-project/data_lan/moods_data/"  
    mood_data = spark.read.option("basePath", mood_data_path).parquet(mood_data_path)

    #top200_path = "/Users/tian_mac/sfu/lab1/cmpt732-spotify-project/top200_year.csv" 
    top200_data = spark.read.option("header", "true").option("inferSchema", "true").csv(top200_path)

    #happy_path = "/Users/tian_mac/sfu/lab1/cmpt732-spotify-project/econ-happiness-cleaned/"
    happy_data = spark.read.option("basePath", happy_path).parquet(happy_path)

    mood_data.printSchema()
    top200_data.printSchema()
    happy_data.printSchema()
    happy_data.show(10)

    # left join the datasets on track_id for inspection
    songs_inspection = top200_data.join(
        mood_data,
        top200_data["track_id"] == mood_data["track_id"],
        how="left"
    )
    # fill na in mood data with "Unknown"
    #songs_complete = songs_complete.fillna({"mood": "Unknown", "language":"Unknown"})
    songs_inspection.drop("average_streams").show(10)

    # Since there are na in mood and languages due to limit on lyrics data, we use 
    #   inner join for actual usegae

    # inner join (what we will work on)
    songs_complete = top200_data.join(
        mood_data,
        top200_data["track_id"] == mood_data["track_id"],
        how="inner"
    )

    tidy_df = songs_complete.drop("duration", "explicit", "total_streams", "weeks_on_chart", "average_streams") \
                            .drop(mood_data["track_id"])
    tidy_df = tidy_df.orderBy("country", "year")
    tidy_df.show(20)

    # count how many songs we now have for each country each year
    count_check_df = tidy_df.groupBy("country", "year").agg(count("track_id").alias("track_counts")) \
                            .orderBy("year", "country")
    count_check_df.show(200)

    # Since lowest track count is 114 for Thailand at 2018, we keep 110 songs for 
    #   our analysis
    window = Window.partitionBy("country", "year").orderBy(col("track_id"))
    # Add a row number to each song within its country-year group
    ranked_data = tidy_df.withColumn("new_rank", row_number().over(window))
    # Filter to retain only the top 110 songs per country per year
    filtered_df = ranked_data.filter(col("new_rank") <= 110).drop("rank", "new_rank")
    filtered_df.show(20)

    # now we count the number of positive/neural/neative mood per year per country
    mood_count_df = filtered_df.groupBy("year", "country", "mood") \
                            .agg(count("track_id").alias("mood_counts"))
    mood_count_df = mood_count_df.groupBy("year", "country") \
                                .pivot("mood", ["positive", "neutral", "negative"]).sum("mood_counts")
    mood_count_df.show(20)

    # join with happiness data
    final_df = mood_count_df.join(happy_data,
                                [mood_count_df["country"] == happy_data["country_code"],
                                mood_count_df["year"] == happy_data["year"]],
                                how="inner") \
                            .select(mood_count_df.year,happy_data.country,
                                    mood_count_df.positive, mood_count_df.neutral,
                                    mood_count_df.negative,happy_data.happiness,
                                    happy_data.developed_country)
    final_df.show(20)

    # output as csv; more analysis in R
    #final_df.coalesce(1).write.mode("overwrite").csv("/Users/tian_mac/sfu/lab1/project/happiness_w_mood_R", header= True)
    final_df.coalesce(1).write.mode("overwrite").csv(output, header= True)

if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    input3 = sys.argv[3]
    outputs = sys.argv[4]
    main(input1,input2, input3, outputs)                          