
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F

song_schema = types.StructType([
        types.StructField('country', types.StringType()),
        types.StructField('year', types.IntegerType()),
        types.StructField('track_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('artists', types.StringType()),
        types.StructField('duration', types.LongType()),
        types.StructField('artist_genres', types.StringType()),
        types.StructField('explicit', types.StringType()),
        types.StructField('total_streams', types.LongType()),
        types.StructField('weeks_on_chart', types.IntegerType()),
        types.StructField('average_streams', types.LongType()),
        types.StructField('rank', types.IntegerType()),
    ])

###
###     This code is to seperate the top200_data
###     It will first combine with the database with lyrics. -> with_lyr folder
###     Then, grab non_lyrics data  -> no_lyr folder
###



def main(inputs,inp,output,output2):
    df = spark.read.csv(inputs,sep=',',schema=song_schema,header=True)
    df1 = df.select(df['track_id'],df['artists'],df['name'])
    df1.createOrReplaceTempView('data')
    songs = spark.sql("""
    SELECT Distinct * FROM data
""").cache()



    dn = spark.read.csv(inp,header=True, inferSchema=True, multiLine=True, escape='"')  

    dn.createOrReplaceTempView('goup')
    combined_df = spark.sql("""
    SELECT max(title) AS title, id, 
            CONCAT_WS(' ', COLLECT_LIST(lyrics)) AS lyrics,
            max(language) AS language
    FROM goup
    WHERE tag !='misc'
    group by id
""")

    
    combined_df.createOrReplaceTempView('d1')
    songs.createOrReplaceTempView('d2')

    result = spark.sql("""
    SELECT track_id, d2.name, d2.artists, d1.lyrics, d1.language
    FROM d2 left join d1 on d2.name = d1.title
""").cache()
    
    result.createOrReplaceTempView('table')
    new = spark.sql("""
    SELECT track_id,
            CONCAT_WS(' ', COLLECT_LIST(lyrics)) AS lyrics,
            max(language) AS language
    FROM table
    where lyrics is NOT NULL AND language is NOT NULL
    GROUP BY track_id
""")
    
    non_ly = spark.sql("""
    SELECT DISTINCT track_id, name, artists
    FROM d2 
    WHERE track_id not in (
        SELECT DISTINCT track_id
        FROM table
        where lyrics is NOT NULL AND language is NOT NULL
      )
""")
    
    df2 = non_ly.withColumn('new_artist',F.split(non_ly["artists"], "\'").getItem(1))
    df_new = df2.select(df2['track_id'],df2['name'],df2['new_artist'])

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df3 = df_new.withColumn("row_index", F.row_number().over(window_spec))

    df4 = df3.withColumn("batch_id", ((F.col("row_index") - 1) / 50).cast("int"))

    df4.write.partitionBy("batch_id").mode("overwrite").csv(output2,header=True)

    
    new.write.json(output, mode="overwrite")
    #non_ly.coalesce(1).write.csv(output2, mode="overwrite")

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    inp =sys.argv[2]
    output = sys.argv[3]
    output2 = sys.argv[4]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    #main(inputs, output)
    main(inputs,inp,output,output2)
