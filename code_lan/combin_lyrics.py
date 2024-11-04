
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

song_schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('artists', types.StringType()),
    ])

schema = types.StructType([
        types.StructField('title', types.StringType()),
        types.StructField('id', types.LongType()),
        types.StructField('lyrics', types.StringType()),
        types.StructField('language', types.StringType()),
    ])


def main(inputs,inp,output,output2):

    df = spark.read.json(inputs,schema=song_schema).cache()

    dn = spark.read.json(inp,schema=schema)  

    
    dn.createOrReplaceTempView('d1')
    df.createOrReplaceTempView('d2')

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
    
    new.coalesce(1).write.json(output, mode="overwrite")
    non_ly.coalesce(1).write.csv(output2, mode="overwrite")

    

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
