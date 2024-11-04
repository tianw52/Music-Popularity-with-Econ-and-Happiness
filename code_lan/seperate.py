
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F

song_schema = types.StructType([
        types.StructField('track_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('artists', types.StringType()),
    ])



def main(inputs,output):

    df = spark.read.csv(inputs,header=False, inferSchema=True,multiLine=True, escape='"')
    custom_columns = ["track_id", "name", "artists"] 
    df1 = df.toDF(*custom_columns)
    df2 = df1.withColumn('new_artist',F.split(df1["artists"], "\'").getItem(1))
    df_new = df2.select(df2['track_id'],df2['name'],df2['new_artist'])

    window_spec = Window.orderBy(F.monotonically_increasing_id())
    df3 = df_new.withColumn("row_index", F.row_number().over(window_spec))

    df4 = df3.withColumn("batch_id", ((F.col("row_index") - 1) / 50).cast("int"))

    df4.write.partitionBy("batch_id").mode("overwrite").csv(output, header=True)

    

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    #main(inputs, output)
    main(inputs,output)
