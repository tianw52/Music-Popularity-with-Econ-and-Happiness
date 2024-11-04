
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row

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


def main(inputs,output):

    df = spark.read.csv(inputs,sep=',',schema=song_schema,header=True)
    df1 = df.select(df['track_id'],df['artists'],df['name'])
    df1.createOrReplaceTempView('data')
    songs = spark.sql("""
    SELECT Distinct * FROM data
""")

    songs.write.json(output, mode="overwrite")

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    #main(inputs, output)
    main(inputs,output)
