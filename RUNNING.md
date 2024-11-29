# Instructions to run our project

## Which libraries are required?

In addition to the usual PySpark setup required for this course, make sure that you also have the following libraries before running our code:

- `os`
- `crealytics`
- `matplotlib`
- `seaborn`
- `numpy`
- `pandas`
- `python-dotenv`
- `langdetect`
- `lyricsgenius`
- `transformers`
- `torch`
- `pycld2`


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

## Language & Lyrics Data:

### Required input file:
- **[`spotify_clean`](cleaned_data/spotify_clean)**: A directory with the csv file that contains metadata (track ID, artists, duration, artist genres, total number of streams, average number of streams, number of weeks on chart, and our ranking out of 200) about the top 200 Spotify tracks per country and year.
- **[`lyrics dataset`](https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information)**: Dataset with some lyrics from genius website.

We will separate the songs with the existing lyrics dataset into [`with_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/with_lyr) and [`no_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/no_lyr) part by running on Amzon AWS EMR.

```
s3://project-spotify-songs/seperate_data1.py
--conf spark.yarn.maxAppAttempts=1
s3://project-spotify-songs/top200_song.csv s3://project-spotify-songs/lyrics_dataset.csv s3://project-spotify-songs/Sep_data_by_with_or_no_ly/with_lyr  s3://project-spotify-songs/Sep_data_by_with_or_no_ly/no_lyr 
```

For those missing lyrics part, we will use Genous API to grab.
#### Running the Genius API 
1. Create a account for [Genius API](https://docs.genius.com/)
2. Create a New API Client in that account.
3. Paste the 'API SECRET KEY' into the file [`get_lyrics_language.py`](ETL/language_etl/get_lyrics_language2.py)
4. Run the code

Then, we use two language library in python to detect the lyrics language: [`langdetect`](https://pypi.org/project/langdetect/) and [`pycld2`](https://pypi.org/project/pycld2/)

```
spark-submit ETL/get_lyrics_language2.py cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/no_lyr cleaned_data/languages_and_mood/data_with_lyrics&lan
spark-submit ETL/check_lan_miss3.py cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/with_lyr cleaned_data/languages_and_mood/data_with_lyrics&lan
spark-submit ETL/sep_by_lan4.py cleaned_data/languages_and_mood/data_with_lyrics&lan cleaned_data/languages_and_mood cleaned_data/languages_and_mood/parquet_by_lan
```


### Output files produced:

- **[`parquet_by_lan`](cleaned_data/languages_and_mood/parquet_by_lan)**: directory, which contains Parquet files hive-partitioned by language with track_id, language, lyrics


## Sentiment Analysis:
### Required input file:
- **[`parquet_by_lan`](cleaned_data/languages_and_mood/parquet_by_lan)**: directory, which contains Parquet files hive-partitioned by language with track_id, language, lyrics

We use [Hugging Face](https://huggingface.co/) Model to predict our lyrcis moods, by seperate into 3 categories.

- **[`Detect Bulgarian`](https://huggingface.co/DGurgurov/xlm-r_bulgarian_sentiment)**: Code is in [sentiment_bg.py](sentiment_analysis_code/sentiment_bg.py)
- **[`Detect Enilish, Dutch, German, French, Italian, Spanish `](https://huggingface.co/nlptown/bert-base-multilingual-uncased-sentiment)**: Code is in [sentiment_enfritesnl.py](sentiment_analysis_code/sentiment_enfritesnl.py)
- **[`Detect Japanese,Chinese,Malay,Indonesian,Portuguese`](https://huggingface.co/lxyuan/distilbert-base-multilingual-cased-sentiments-student)**: Code is in [sentiment_jazhmsidpt.py](sentiment_analysis_code/sentiment_jazhmsidpt.py)
- **[`Detect Korean`](https://huggingface.co/WhitePeak/bert-base-cased-Korean-sentiment)**: Code is in [sentiment_ko.py](sentiment_analysis_code/sentiment_ko.py)
- **[`Detect Polish`](https://huggingface.co/Voicelab/herbert-base-cased-sentiment)**: Code is in [sentiment_polish.py](sentiment_analysis_code/sentiment_polish.py)
- **[`Detect Romanian`](https://huggingface.co/nico-dv/sentiment-analysis-model)**: Code is in [sentiment_ro.py](sentiment_analysis_code/sentiment_ro.py)
- **[`Detect Russian`](https://huggingface.co/blanchefort/rubert-base-cased-sentiment)**: Code is in [sentiment_ru.py](sentiment_analysis_code/sentiment_ru.py)
- **[`Detect Slovak`](https://huggingface.co/kinit/slovakbert-sentiment-twitter)**: Code is in [sentiment_slovak.py](sentiment_analysis_code/sentiment_slovak.py)
- **[`Detect Swedish`](https://huggingface.co/KBLab/robust-swedish-sentiment-multiclass)**: Code is in [sentiment_sv.py](sentiment_analysis_code/sentiment_sv.py)
- **[`Detect Tagalog`](https://huggingface.co/dost-asti/RoBERTa-tl-sentiment-analysis)**: Code is in [sentimenttagalog.py](sentiment_analysis_code/sentiment_tagalog.py)
- **[`Detect Thai`](https://huggingface.co/SandboxBhh/sentiment-thai-text-model)**: Code is in [sentiment_th.py](sentiment_analysis_code/sentiment_th.py)
- **[`Detect Turkish`](https://huggingface.co/moarslan/bert-base-turkish-sentiment-analysis)**: Code is in [sentiment_tr.py](sentiment_analysis_code/sentiment_tr.py)
- **[`Detect Vietnamese`](https://huggingface.co/mr4/phobert-base-vi-sentiment-analysis)**: Code is in [sentiment_vi.py](sentiment_analysis_code/sentiment_vi.py)


### Output files produced:

- **[`moods data`](cleaned_data/languages_and_mood/moods_data/)**: directory, which contains Parquet files hive-partitioned by language with track_id, moods, lyrics



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
spark-submit Task-2/language_distribution.py cleaned_data/languages_and_mood/parquet_by_lan cleaned_data/spotify_clean Task-2/word_cloud_masks Task-2/plots
```

### Output files produced:

All the plots created in Task 2 can be found in the [`Task-2/plots`](Task-2/plots) directory, which contains:

- Two heatmaps for the language distirbution
- Country-shaped wordclouds of language frequency of each country


## Task 3: Is there a correlation between the popularity of song moods—such as happy music or sad music—to the average happiness of people in different countries?

Using the language, mood, songs, and happiness data we obtained before, we created a dataset used for analysis in R.

```
spark-submit Task-3/prep_task3.py cleaned_data/languages_and_mood/moods_data cleaned_data/spotify_clean cleaned_data/econ_happiness_cleaned Task-3/happiness_w_mood_R
```

### Output files produced:

- **[`Task-3/happiness_w_mood_R`](Task-3/happiness_w_mood_R)**: Directory that contains the CSV file to be used in R later.


### Instructions for R:

- In your preferred R IDE (eg. RStudio), load the `rmd` file **[`Task-3/Task3.rmd`](Task-3/Task3.rmd)**
- Set **[`Task-3/happiness_w_mood_R`](Task-3/happiness_w_mood_R)** as your working directory and load the data

```
df <- read.csv("happiness_w_mood_R/happiness_w_mood_R.csv")
```

- Click knit, a pdf file will be outputed **[`Task-3/Task3.pdf`](Task-3/Task3.pdf)**
