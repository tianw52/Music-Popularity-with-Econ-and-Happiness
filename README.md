# CMPT 732 Final Project: Trends in Song Popularity from Global Phenomena
**Group members**: Tianyi Wu, Qingrui (Rachel) Li, Rebekah Wong

Welcome to our repository! Our project is an exploration of recent trends in song popularity based on global phenomena.

## Instructions to run our project

Please see [`RUNNING.md`](RUNNING.md).

## Pre-etl data

All the raw data about Economics factors and happiness scores can be found in the [pre-etl_data](pre-etl_data) directory, which contains:

- **[`gdp_per_capita`](pre-etl_data/gdp_per_capita) directory**: Contains an Excel file for each country that contains their annual GDP per capita in USD (source: [Statista](https://www.statista.com/)).
- **[`inflation`](pre-etl_data/inflation) directory**: Contains an Excel file for each country that contains their annual inflation rate (source: [Statista](https://www.statista.com/)).
- **[`unemployment`](pre-etl_data/unemployment) directory**: Contains an Excel file for each country that contains their annual unemployment rate (source: [Statista](https://www.statista.com/)).
- **[`happiness.xls`](pre-etl_data/happiness.xls)**: An Excel file that contains the annual happiness scores out of 10 for each country (source: [World Happiness Report](https://worldhappiness.report/data/)).

**Note**: There are two dataset for Spotify tracks is too big for GitHub and must be downloaded from Kaggle:
- **[`Song Dataset`](https://www.kaggle.com/datasets/jfreyberg/spotify-chart-data)**: Database with details about top songs in each country per week in each year.
- **[`Lyrics Dataset`](https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information)**: Database with some lyrics from genius website.



## Language & Lyrics Data:

We will separate the songs with the existing lyrics dataset into [`with_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/with_lyr) and [`no_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/no_lyr) part by running on Amzon AWS EMR.

For those missing lyrics part, we will use Genous API to grab.

Then, we use two language library in python to detect the lyrics language: [`langdetect`](https://pypi.org/project/langdetect/) and [`pycld2`](https://pypi.org/project/pycld2/)

- **[`parquet_by_lan`](cleaned_data/languages_and_mood/parquet_by_lan)**: directory, which contains Parquet files hive-partitioned by language with track_id, language, lyrics



## Sentiment Analysis
We use [Hugging Face](https://huggingface.co/) Model to predict our lyrcis moods, by seperate into 3 categories.
All the related code in that [file](sentiment_analysis_code) is to run the Sentiment Analysis.

## POWERBI VISUALIZATION:
We have a [powerbi dashborad](BI_dashboard/Visualization.pbix) to show what we found based on our cleaned_data.
(You can download and upload them in your sfu powerbi website to see and try it.)
