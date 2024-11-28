# Instructions to run our project

## Which libraries are required?

In addition to the usual PySpark setup required for this course, make sure that you also have the following libraries before running our code:

- `os`
- `crealytics`
- `matplotlib`
- `seaborn`
- `numpy`
- `pandas`


## How do we run the project?

To run our project, please use these commands:

## Extract-Transform-Load

First, let's combine our economic attributes (GDP per capita, inflation rate, unemployment rate) with the happiness score per country and year:

```
spark-submit --packages com.crealytics:spark-excel_2.12:0.13.5 ETL/econ_happiness_etl.py pre-etl_data cleaned_data/econ_happiness_cleaned
```

### Required input files:

All of the raw inputs required can be found in the [`pre-etl_data`](pre-etl_data) directory, which contains:

- **[`gdp_per_capita`](pre-etl_data/gdp_per_capita) directory**: Contains an Excel file for each country that contains their annual GDP per capita in USD (source: [Statista](https://www.statista.com/)).
- **[`inflation`](pre-etl_data/inflation) directory**: Contains an Excel file for each country that contains their annual inflation rate (source: [Statista](https://www.statista.com/)).
- **[`unemployment`](pre-etl_data/unemployment) directory**: Contains an Excel file for each country that contains their annual unemployment rate (source: [Statista](https://www.statista.com/)).
- **[`happiness.xls`](pre-etl_data/happiness.xls)**: An Excel file that contains the annual happiness scores out of 10 for each country (source: [World Happiness Report](https://worldhappiness.report/data/)).


### Output files produced:

The [`cleaned_data/econ_happiness_cleaned`](cleaned_data/econ_happiness_cleaned) directory, which contains Parquet files hive-partitioned by country with economic data and happiness scores per year.

---

Next, let's find the top 200 Spotify songs per country and year:

**Note**: The dataset for Spotify tracks is too big for GitHub and must be downloaded from [Kaggle](https://www.kaggle.com/datasets/jfreyberg/spotify-chart-data). Please adjust your input path accrodingly below.

```
spark-submit ETL/spotify_year.py path/to/your/kaggle/download cleaed_data/spotify_top200
```

### Output files produced:

- **[`cleaned_data/spotify_clean`](cleaned_data/spotify_clean)**: A directory with the csv file that contains metadata (track ID, artists, duration, artist genres, total number of streams, average number of streams, number of weeks on chart, and our ranking out of 200) about the top 200 Spotify tracks per country and year.

---

Using [`cleaned_data/spotify_clean`](cleaned_data/spotify_clean) from the previous step, we need to extract the artist genres for each country and year:

```
spark-submit ETL/genre_artist_etl.py cleaned_data/spotify_clean cleaned_data/genres_by_artist
```

### Output files produced:

The [`cleaned_data/genres_by_artist`](cleaned_data/genres_by_artist) directory, which contains Parquet files hive-partitioned by country with top artist genres per year.

---

We also need to find more specific song genres for the top 10 songs per country and year, because the actual song may be written in a different genre than the generalized genre of the artist:

```
spark-submit ETL/genre_song_etl.py cleaned_data/top10_genres.csv cleaned_data/genres_by_song
```

### Required input file:

- **[`cleaned_data/top10_genres.csv`](cleaned_data/top10_genres.csv)**: A CSV file that contains information about the actual song genre of the top 10 songs per country and year. Created by manually looking up song genres from [`cleaned_data/spotify_clean`](cleaned_data/spotify_clean) on [Rate Your Music](https://rateyourmusic.com/), one by one (source: us!)

### Output files produced:

The [`cleaned_data/genres_by_song`](cleaned_data/genres_by_song) directory, which contains Parquet files hive-partitioned by country with top song genres per year.


## Task 1: To what extent is the popularity of songs affected by economic factors—namely GDP, unemployment rates, and inflation?

```
spark-submit Task-1/economic_analysis.py cleaned_data/econ_happiness_cleaned cleaned_data/genres_by_song cleaned_data/genres_by_artist Task-1/output
```

### Output files produced:

All of the Task 1 outputs can be found in the [`Task-1/output`](Task-1/output) directory, which contains:

- **[`top10_song_genres`](Task-1/output/top10_song_genres) directory**: Contains stacked bar charts of the genre counts for the top 10 songs per country and year, graphed alongside the economic attributes.
- **[`top150_artist_genres`](Task-1/output/top150_artist_genres) directory**: Contains stacked bar charts of the genre counts for the top 150 songs' artists per country and year, graphed alongside the economic attributes.
- **[`econ_artist_genre_heatmap.png`](Task-1/output/econ_artist_genre_heatmap.png)**: Heatmap of the correlation matrix between each artist genre and the economic attributes across all countries and years.
- **[`econ_song_genre_heatmap.png`](Task-1/output/econ_song_genre_heatmap.png)**: Heatmap of the correlation matrix between each song genre and the economic attributes across all countries and years.


## Task 2: Have songs written in certain languages risen in popularity internationally?

```
spark-submit Task-2/language_distribution.py Task-2/language_all.json cleaned_data/spotify_clean Task-2/word_cloud_masks Task-2/plots
```

### Output files produced:

All the plots created in Task 2 can be found in the [`Task-2/plots`](Task-2/plots) directory, which contains:

- Two heatmaps for the language distirbution
- Country-shaped wordclouds of language frequency of each country


## Task 3: Is there a correlation between the popularity of song moods—such as happy music or sad music—to the average happiness of people in different countries?

Using the language, mood, songs, and happiness data we obtained before, we created a dataset used for analysis in R.

```
prep_task3.py cleaned_data/languages_and_mood/moods_data cleaned_data/spotify_clean cleaned_data/econ_happiness_cleaned Task-3/happiness_w_mood_R
```

### Output files produced:

- **[`Task-3/happiness_w_mood_R`](Task-3/happiness_w_mood_R)**: Directory that contains the CSV file to be used in R later.
