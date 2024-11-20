import sys, matplotlib.pyplot as plt, numpy as np, pandas as pd, seaborn as sb
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

def bar_with_line(dataframe, song_df, economic_df, country, output):
    # Can convert to Pandas here because DataFrame has already
    # been heavily reduced by country into one row/year,
    # so over 5 years there's only 5 rows of data = small!
    dataframe = dataframe.toPandas()
    song_df = song_df.toPandas()
    economic_df = economic_df.toPandas()

    dataframes = [dataframe, song_df]
    artist_data = True

    for df in dataframes:
        # Initialize subplots to graph two sets of data in the same
        # plot, and twinx to add another vertical axis on the right
        # https://stackoverflow.com/a/58016508
        figure, axis = plt.subplots()
        axis2 = plt.twinx(ax=axis)

        if (artist_data == True):
            # https://www.geeksforgeeks.org/create-a-stacked-bar-plot-in-matplotlib/
            # Set custom colour palette to avoid duplicate colours in bars
            # https://stackoverflow.com/a/66339676
            df.plot(x="year", kind="bar", stacked=True, cmap="tab20", \
                                    ylabel="count", title=country + " popular " \
                                    "song artist genres (Top 150 songs)", ax=axis)
        
        # Need to label the graph differently depending on artist or song genre
        else:
            df.plot(x="year", kind="bar", stacked=True, cmap="tab20", \
                                    ylabel="count", title=country + " popular " \
                                    "song genres (Top 10 songs)", ax=axis)

        # Set bbox_to_anchor to move the legend beside the graph instead of on top
        axis.legend(bbox_to_anchor=(1.5, 1))

        # Plot the economic data as lines on top of the bars
        economic_df[["gdp_per_capita (in 10,000s)", "inflation_rate", \
                    "unemployment_rate"]].plot(ylabel="percent (inflation," \
                    " unemployment) or 10k USD (GDP)", ax=axis2)

        # Set position of lines legend
        axis2.legend(bbox_to_anchor=(1.5, 1.25))

        # Label count value on the bar itself for clarity
        # https://stackoverflow.com/a/63856446
        for bar in axis.containers:
            # Only label the bar if it's above a certain number
            # (in this case, 10) to avoid the labels overlapping
            # https://stackoverflow.com/a/69404281
            labels = [int(count) if count >= 10 else "" for count in bar.datavalues]
            axis.bar_label(bar, labels=labels, label_type="center")

        if (artist_data == True):
            # Note: output directory must already exist
            plt.savefig(output + "/top150_artist_genres/" + country \
                        + ".png", bbox_inches="tight")

        else:
            plt.savefig(output + "/top10_song_genres/" + country \
                        + ".png", bbox_inches="tight")
        
        # Close plot after saving to free memory
        plt.close()

        artist_data = False

    return

def main(econ, genre_song, genre_artist, output):
    # Read cleaned Parquet data from previous steps
    # Remove happiness column as not relevant for this question
    # "country" column is the 2-letter abbreviation in the other
    # 2 dataests, so rename economic data column for consistency
    # Cache all input datasets because they'll be used at least twice each
    economic_data = spark.read.parquet(econ).drop("happiness") \
                        .withColumnRenamed("country", "full_country_name") \
                        .withColumnRenamed("country_code", "country") \
                        .withColumnRenamed("developed_country", \
                                        "is_developed_country").cache()

    top10_song_genres = spark.read.parquet(genre_song).cache()
    top150_artist_genres = spark.read.parquet(genre_artist).cache()



    # PART 1 - TOP SONG GENRE & TOP 3 ARTIST GENRES
    # (not sure if this will be used anywhere)

    # Use max_by function to find genre associated with max count directly
    # https://stackoverflow.com/a/72683831
    most_popular_song_genre = top10_song_genres.groupBy(["country", "year"]) \
                                .agg(functions.max_by("genre", "count")) \
                                .withColumnRenamed("max_by(genre, count)", \
                                "most_popular_genre")

    popular_song_counts = most_popular_song_genre \
                            .groupBy(["most_popular_genre", "year"]).count() \
                            .orderBy(["year", "count"], ascending=[True, False]) \
                            .withColumnRenamed("count", \
                            "# of countries (incl. global)")

    # Save most popular song genre per country/year, in case it's relevant
    popular_song_counts.write.csv(output + "/csv/top1_song_genre", mode="overwrite")

    # Ignore distribution of explicit/non-explicit songs for now
    # Used multiple times, so cache
    filtered_artist_genres = top150_artist_genres.filter \
                                ((top150_artist_genres["genre"] != "explicit") & \
                                (top150_artist_genres["genre"] != "non-explicit")) \
                                .cache()

    # Use window function to easily rank and filter
    # the top 3 song genres for each country
    # https://stackoverflow.com/a/38398563
    top_songs_window = window.Window.partitionBy(["country", "year"]) \
                            .orderBy(filtered_artist_genres["count"].desc())

    # Song data is already sorted in descending order,
    # so apply a ranking to our genres in order
    # and only keep the top 3 genres per country/year
    top_song_ranking = filtered_artist_genres \
                        .select("*", functions.rank() \
                        .over(top_songs_window) \
                        .alias("ranking")) \
                        .filter(functions.col("ranking") <= 3)

    # Save information about top 3 songs per country/year, in case it's relevant
    top_song_ranking.write.csv(output + "/csv/top3_artist_genre", mode="overwrite")



    # PART 2 - GRAPHS/VISUALIZATIONS OF ECONOMIC ATTRIBUTES WITH SONG GENRES

    # Pivot data so that we can have columns for each genre
    # https://stackoverflow.com/a/71900288
    column_genres = top150_artist_genres.groupBy(["year", "country"]) \
                                        .pivot("genre").sum("count") \
                                        .orderBy(["year", "country"]) \

    # Song genres data formatted slightly differently,
    # where we get NULLs for genres that weren't recorded in top 10
    # Replace NULLs with 0
    column_song_genres = top10_song_genres.groupBy(["year", "country"]) \
                                            .pivot("genre").sum("count") \
                                            .orderBy(["year", "country"]) \
                                            .fillna(value=0)

    # Combine artist genres with economic data,
    # but remove columns that are non-numerical or
    # won't be relevant to the correlation
    # This DataFrame is used twice, so cache
    artist_genres_econ = column_genres.join(economic_data, \
                                    on=["year", "country"]) \
                                    .drop("global_inflation_rate", \
                                    "global_unemployment_rate", \
                                    "country") \
                                    .cache()

    song_genres_econ = column_song_genres.join(economic_data, \
                                        on=["year", "country"]) \
                                        .drop("global_inflation_rate", \
                                        "global_unemployment_rate", \
                                        "country") \
                                        .cache()

    # Extract all countries and years from the columns
    # https://stackoverflow.com/a/60896261
    countries = artist_genres_econ.select("full_country_name").distinct().collect()
    countries = [country[0] for country in countries]

    # Separate economic data to graph onto a line on top of bar chart
    economic = economic_data[["year", "full_country_name", "gdp_per_capita", \
                            "inflation_rate", "unemployment_rate"]]

    # Scale data so that it fits more neatly from 0 - 10
    # so that it can be compared to inflation and unemployment %
    econ_to_graph = economic.withColumn("gdp_per_capita (in 10,000s)", \
                                        economic["gdp_per_capita"] / 10000) \
                                        .drop("gdp_per_capita")

    # Exclude any non-musical genres from bar chart
    data_to_graph = artist_genres_econ.drop("explicit", "non-explicit", \
                                            "is_developed_country", \
                                            "gdp_per_capita", \
                                            "inflation_rate", \
                                            "unemployment_rate")

    song_data_to_graph = song_genres_econ.drop("is_developed_country", \
                                                "gdp_per_capita", \
                                                "inflation_rate", \
                                                "unemployment_rate")

    # Go through every country and graph the artist/song genre data
    # with economic attributes per country/year
    for country in countries:
        data = data_to_graph.filter(data_to_graph["full_country_name"] == country)

        song_data = song_data_to_graph.filter( \
                        song_data_to_graph["full_country_name"] == country)

        econ_data = econ_to_graph.filter(econ_to_graph["full_country_name"] == country)

        bar_with_line(data, song_data, econ_data, country, output)



    # PART 3 - CORRELATION/HEATMAP

    # No longer needed for numerical correlation section
    artist_genres_econ = artist_genres_econ.drop("year", "full_country_name")
    song_genres_econ = song_genres_econ.drop("year", "full_country_name")

    column_names = artist_genres_econ.columns
    song_column_names = song_genres_econ.columns

    # Need to convert DataFrame into Vector format to use correlation function
    # https://stackoverflow.com/a/52217904
    vector_assembler = VectorAssembler(inputCols=column_names, \
                                        outputCol="features")

    song_vector_assembler = VectorAssembler(inputCols=song_column_names, \
                                            outputCol="song_features")

    artist_genres_econ_vector = vector_assembler.transform(artist_genres_econ) \
                                                .select("features")

    song_genres_econ_vector = song_vector_assembler.transform(song_genres_econ) \
                                                    .select("song_features")

    # Find correlation matrix between all genres and economic attributes
    genre_correlations = Correlation.corr(artist_genres_econ_vector, "features")

    song_genre_correlations = Correlation.corr(song_genres_econ_vector, \
                                                "song_features")

    # We have a fixed number of genres and economic attributes,
    # so we can collect at this stage to get the array output
    # Continuation of https://stackoverflow.com/a/52217904 from above
    correlation_matrix = genre_correlations.collect()[0] \
                            ["pearson({})".format("features")].values

    song_correlation_matrix = song_genre_correlations.collect()[0] \
                                ["pearson({})".format("song_features")].values

    # Correlation matrix is a square with size corresponding to
    # (number of genres or economic types)^2, so the length of
    # each row and column is the same as the number of columns
    # we originally had
    row_col_size = len(column_names)
    song_row_col_size = len(song_column_names)

    # Fix the size of our correlation matrix so that it is
    # a proper square instead of having uneven row and column lengths
    artist_genre_correlation = np.reshape(correlation_matrix, \
                                    (row_col_size, row_col_size))

    song_genre_correlation = np.reshape(song_correlation_matrix, \
                                (song_row_col_size, song_row_col_size))

    # Plot and save heatmap of our correlation results
    # Set bbox_inches to pad the figure enough such that
    # the labels don't get cut off
    heatmap = sb.heatmap(artist_genre_correlation,
                            cmap=sb.color_palette("magma", as_cmap=True), \
                            xticklabels=column_names, \
                            yticklabels=column_names) \
                            .set_title("Correlation between different" \
                            " artist types and economic attributes" \
                            " (Top 150 songs)") \
                            .figure.savefig(output + "/econ_artist_genre" \
                            "_heatmap.png", bbox_inches="tight")

    # Heat map for top 10 song genres per year/country, but
    # because there's too many unique genres and too little data to work with,
    # this heatmap should be discarded
    song_heatmap = sb.heatmap(song_genre_correlation, cbar=False, \
                                cmap=sb.color_palette("magma", as_cmap=True), \
                                xticklabels=song_column_names, \
                                yticklabels=song_column_names) \
                                .set_title("Correlation between different" \
                                " song types and economic attributes" \
                                " (Top 10 songs)") \
                                .figure.savefig(output + "/econ_song_genre" \
                                "_heatmap.png", bbox_inches="tight")

# $ spark-submit economic_analysis.py ../econ-happiness-cleaned
# ../genres_by_song ../genres_by_artist output
if __name__ == '__main__':
    econ = sys.argv[1]
    genre_song = sys.argv[2]
    genre_artist = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('Economic analysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(econ, genre_song, genre_artist, output)