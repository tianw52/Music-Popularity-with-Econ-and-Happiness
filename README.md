# CMPT 732 Final Project: Trends in Song Popularity from Global Phenomena
**Group members**: Tianyi Wu, Qingrui (Rachel) Li, Rebekah Wong

Welcome to our repository! Our project is an exploration of recent trends in song popularity based on global phenomena.

## Instructions to run our project

Please see [`RUNNING.md`](RUNNING.md).

## Pre-ETL Data

All the raw data about economic factors and happiness scores can be found in the [pre-etl_data](pre-etl_data) directory, which contains:

- **[`gdp_per_capita`](pre-etl_data/gdp_per_capita) directory**: Contains an Excel file for each country that contains their annual GDP per capita in USD (source: [Statista](https://www.statista.com/)).
- **[`inflation`](pre-etl_data/inflation) directory**: Contains an Excel file for each country that contains their annual inflation rate (source: [Statista](https://www.statista.com/)).
- **[`unemployment`](pre-etl_data/unemployment) directory**: Contains an Excel file for each country that contains their annual unemployment rate (source: [Statista](https://www.statista.com/)).
- **[`happiness.xls`](pre-etl_data/happiness.xls)**: An Excel file that contains the annual happiness scores out of 10 for each country (source: [World Happiness Report](https://worldhappiness.report/data/)).

**Note**: There are two datasets for Spotify tracks which are too big for GitHub and must be downloaded from Kaggle:
- **[`Song Dataset`](https://www.kaggle.com/datasets/jfreyberg/spotify-chart-data)**: Database with details about top songs in each country per week in each year.
- **[`Lyrics Dataset`](https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information)**: Database with some lyrics from the Genius website.



## Language & Lyrics Data

We will separate the songs that have and do not have existing lyrics from the Kaggle dataset into [`with_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/with_lyr) and [`no_lyrics`](cleaned_data/languages_and_mood/Sep_data_by_with_or_no_ly/no_lyr) respectively and run it on Amazon AWS EMR.

We will also use the [Genius API](https://docs.genius.com/) to grab songs with missing lyrics.

Then, we use two language libraries in Python to detect the language of the lyrics: [`langdetect`](https://pypi.org/project/langdetect/) and [`pycld2`](https://pypi.org/project/pycld2/).

### Output

- **[`parquet_by_lan`](cleaned_data/languages_and_mood/parquet_by_lan)**: directory, which contains Parquet files hive-partitioned by language with track_id, language, and lyrics.



## Sentiment Analysis
We use [Hugging Face](https://huggingface.co/) models to predict the mood of our lyrics, by separating them into three categories.
All the related code in this [directory](sentiment_analysis_code) is to run the sentiment analysis portion of our project.

## PowerBI Visualization

We also have a [PowerBI dashboard](BI_dashboard/Visualization.pbix) to show what we found based on our cleaned data.
You can download the file and upload it to your [SFU PowerBI workspace](https://app.powerbi.com/groups/me/list?experience=power-bi&clientSideAuth=0) to view and interact with it.
