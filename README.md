# CMPT 732 Final Project: Trends in Song Popularity from Global Phenomena
**Group members**: Tianyi Wu, Qingrui (Rachel) Li, Rebekah Wong

Welcome to our repository! Our project is an exploration of recent trends in song popularity based on global phenomena.

## Instructions to run our project

Please see [`RUNNING.md`](RUNNING.md).

## pre-etl data

All the raw data about Economics factors and happiness scores can be found in the [`pre-etl_data`](pre-etl_data) directory, which contains:

- **[`gdp_per_capita`](pre-etl_data/gdp_per_capita) directory**: Contains an Excel file for each country that contains their annual GDP per capita in USD (source: [Statista](https://www.statista.com/)).
- **[`inflation`](pre-etl_data/inflation) directory**: Contains an Excel file for each country that contains their annual inflation rate (source: [Statista](https://www.statista.com/)).
- **[`unemployment`](pre-etl_data/unemployment) directory**: Contains an Excel file for each country that contains their annual unemployment rate (source: [Statista](https://www.statista.com/)).
- **[`happiness.xls`](pre-etl_data/happiness.xls)**: An Excel file that contains the annual happiness scores out of 10 for each country (source: [World Happiness Report](https://worldhappiness.report/data/)).

**Note**: There are two dataset for Spotify tracks is too big for GitHub and must be downloaded from Kaggle:
- **[`Song Dataset`](https://www.kaggle.com/datasets/jfreyberg/spotify-chart-data)**: Database with details about top songs in each country per week in each year.
- **[`Lyrics Dataset`](https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information)**: Database with some lyrics from genius website.



## POWERBI VISUALIZATION:
We have a [`powerbi dashborad`](BI_dashboard/Visualization.pbix) to show what we found based on our cleaned_data.
(You can download and upload them in your sfu powerbi website to see and try it.)
