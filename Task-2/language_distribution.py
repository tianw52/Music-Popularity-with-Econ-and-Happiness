from pyspark.sql import SparkSession
import country_converter as coco
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import langcodes as lc
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import numpy as np
from PIL import Image, ImageFilter
import os
import unicodedata
import sys


# wordcloud
def clouding(data, title, mask = None, save_path = None):
  # log transformation to give less frequent languages higher weights
  #   so they appear bigger in the wordcloud
  transformed_counts = {word: np.log1p(freq) for word, freq in data.items()}
  wordcloud = WordCloud(scale=3,
                        #width=800,
                        #height=400,
                        background_color='white',
                        mask=mask,
                        colormap='brg',
                        contour_color='grey',
                        contour_width=2)\
              .generate_from_frequencies(transformed_counts)
  plt.figure(figsize=(12, 10))
  plt.imshow(wordcloud, interpolation='bilinear')
  plt.axis('off')
  plt.title(f"Word Cloud of Language Trends for {title} (2018-2022)")
  if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
  #plt.show()
  
def main(lan_data, song_data, masks_dir, output):

  spark = SparkSession.builder.appName("Task 2 -- Language Distribution").getOrCreate()
  df_lan = spark.read.parquet(lan_data)
  df_songs = spark.read.csv(song_data, header=True)

  df_lan.createOrReplaceTempView("lan")
  df_songs.createOrReplaceTempView("songs")

  # Join the languages data and songs data
  joined_df = spark.sql("""
      select songs.track_id, songs.year, songs.country, lan.language, songs.rank, songs.name
      from songs
      join lan
      on songs.track_id = lan.track_id
      where lan.language != "no"
      order by songs.year, songs.country, lan.language, songs.rank
  """)

  # group by year, country, and language
  joined_df.createOrReplaceTempView("joined_df")
  grouped_df = spark.sql("""
      select year, country, language, count(*) AS track_count
      from joined_df
      group by year, country, language
      order by year, country, track_count ASC
  """)


  # UDF for country code conversion
  convert_country = udf(lambda x: "Global" if x == "global" else coco.convert(names=x, to='name_short'), StringType())
  df_with_full_country = grouped_df.withColumn("country_full", convert_country(grouped_df["country"]))
  # UDF for language code conversion
  convert_language = udf(lambda x: lc.Language.make(x).display_name(), StringType())
  df_with_full_language = df_with_full_country.withColumn("language_full", convert_language(df_with_full_country["language"]))
  # clean up
  df_tidy = df_with_full_language.drop('country', 'language') \
                                .withColumnRenamed("country_full", "country") \
                                .withColumnRenamed("language_full", "language").cache()
  # get global data only                               
  df_global = df_tidy.filter(df_tidy["country"] == "Global").cache()
  # excluding english for analysis purpose
  df_gloabl_no_en = df_global.filter(df_global['language'] != 'English').cache()      
  # convert to pandas for visualization
  pd_global = df_global.toPandas()
  pd_global_no_en = df_gloabl_no_en.toPandas()


  ## ============= Plotting ==========================
  # Line graph of the trend for languages over the years for "Global" data
  # plt.figure(figsize=(14, 8))
  # for language in pd_global['language'].unique():
  #     language_data = pd_global[pd_global['language'] == language]
  #     plt.plot(language_data['year'], language_data['track_count'], marker='o', label=language)

  # plt.title("Trends in Language Usage for Global Data (2018-2022)")
  # plt.xlabel("Year")
  # plt.ylabel("Track Count")
  # plt.legend(title="Language", loc='upper left', bbox_to_anchor=(1, 1))
  # plt.grid(False)
  # plt.show()

  # Heatmap for global (including English)
  heatmap_data = pd_global.pivot_table(index='language', columns='year', values='track_count').fillna(0)
  heatmap_data['Total_Track_Count'] = heatmap_data.sum(axis=1)
  heatmap_data = heatmap_data.sort_values(by='Total_Track_Count', ascending=False).drop(columns=['Total_Track_Count'])

  plt.figure(figsize=(12, 10))
  sns.heatmap(heatmap_data, cmap='Reds', annot=True, linewidths=0.5)
  plt.title("Heatmap of Language Usage (2018-2022)")
  plt.xlabel("Year")
  plt.ylabel("Language")
  #plt.savefig('/Users/tian_mac/sfu/lab1/project/plots/heatmap.png', dpi=300, bbox_inches='tight')
  plt.savefig(os.path.join(output, "heatmap.png"), dpi=300, bbox_inches='tight')

  # Heatmap for gloval (exluding English)
  heatmap_no_en = pd_global_no_en.pivot_table(index='language', columns='year', values='track_count').fillna(0)
  heatmap_no_en['Total_Track_Count'] = heatmap_no_en.sum(axis=1)
  heatmap_no_en = heatmap_no_en.sort_values(by='Total_Track_Count', ascending=False).drop(columns=['Total_Track_Count'])

  plt.figure(figsize=(12, 10))
  sns.heatmap(heatmap_no_en, cmap='Reds', annot=True, linewidths=0.5)
  plt.title("Heatmap of Language Usage Excluding English (2018-2022)")
  plt.xlabel("Year")
  plt.ylabel("Language")
  #plt.savefig('/Users/tian_mac/sfu/lab1/project/plots/heatmap_no_en.png', dpi=300, bbox_inches='tight')
  plt.savefig(os.path.join(output, "heatmap_no_en.png"), dpi=300, bbox_inches='tight')


  # get pictures for the masks for different countries
  #mask_folder_path = '/Users/tian_mac/sfu/lab1/project/word_cloud_masks'  
  mask_folder_path = masks_dir
  masks = {}
  for filename in os.listdir(mask_folder_path):
    if filename.endswith('.png') or filename.endswith('.jpg'):
      country_name = filename.split('.')[0].replace('_', ' ')
      country_name = unicodedata.normalize('NFC', country_name)
      mask_image = Image.open(os.path.join(mask_folder_path, filename))
      # smooth the egde of the map images
      smooth_mask = mask_image.filter(ImageFilter.GaussianBlur(2))  
      masks[country_name] = np.array(smooth_mask)


  countries = df_tidy.select("country").distinct().collect()
  #print(countries)
  # Generating wordclouds
  for country in countries:
      country_name = unicodedata.normalize('NFC', country['country'])
      country_data = df_tidy.filter(df_tidy['country'] == country_name).toPandas()
      language_counts = country_data.set_index('language')['track_count'].to_dict()
      out_path = os.path.join(output, f"{country_name}_wordcloud.png")
      if country_name != "Global": # use masks for countries
          clouding(language_counts, country_name, mask=masks[country_name],
                  save_path=out_path)
      else:
          clouding(language_counts, country_name, None,
                  save_path=out_path)


if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    masks_path = sys.argv[3]
    outputs = sys.argv[4]
    main(input1,input2, masks_path, outputs)        