import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types, Row



def main(inp,output):

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
    
    combined_df.write.json(output, mode="overwrite")
    

if __name__ == '__main__':
    inp =sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('reddit average df').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    sc = spark.sparkContext
    main(inp,output)
