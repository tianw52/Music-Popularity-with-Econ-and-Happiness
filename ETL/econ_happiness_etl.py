import sys, os
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# Filter the economic datasets by year
def filter_econ_data(dataframe):
    # Strip asterisks (*) from some of the year values
    # to create new column with numerical years instead of string
    # https://stackoverflow.com/a/50766974
    # https://stackoverflow.com/a/75634213
    cleaned_years = dataframe.withColumn("year", \
                            functions.regexp_replace( \
                            functions.col("year_string"), "[*]", "") \
                            .cast(types.IntegerType()))
    
    filtered = cleaned_years.filter((cleaned_years["year"] >= 2018) \
                                    & (cleaned_years["year"] <= 2022))

    return filtered

def read_inflation_gdp(countries):
    # Spark Excel: https://github.com/nightscape/spark-excel
    # https://medium.com/@amitjoshi7/
    # how-to-read-excel-files-using-pyspark-in-databricks-637bb21b90be

    num_countries = len(countries)

    inflation_schema = types.StructType([
        types.StructField("year_string", types.StringType()),
        types.StructField("inflation_rate", types.FloatType()),
        types.StructField("country_code", types.StringType()),
        types.StructField("country", types.StringType())
    ])

    # GDP per capita is in US dollars
    gdp_schema = types.StructType([
        types.StructField("year_string", types.StringType()),
        types.StructField("gdp_per_capita", types.FloatType()),
        types.StructField("country_code", types.StringType()),
        types.StructField("country", types.StringType())
    ])

    unemployment_schema = types.StructType([
        types.StructField("year_string", types.StringType()),
        types.StructField("unemployment_rate", types.FloatType()),
        types.StructField("country_code", types.StringType()),
        types.StructField("country", types.StringType())
    ])

    # Make empty DataFrames to store results
    inflation_dataframe = spark.createDataFrame([], schema=inflation_schema)
    gdp_dataframe = spark.createDataFrame([], schema=gdp_schema)
    unemployment_dataframe = spark.createDataFrame([], schema=unemployment_schema)

    for i in range(num_countries):
        # File name format is countrycode_countryname.xlsx
        # Split by underscore to extract code and country name
        country_data = countries[i].split("_")
        country_code = country_data[0]
        country_name = country_data[1][:-5]

        # Read specified Excel sheet and the cells listed
        # B corresponds to year, C corresponds to inflation %
        # The Statista data sheets are formatted similarly, so
        # we can grab the same range of cells
        inflation_data = spark.read.format("com.crealytics.spark.excel") \
                                    .option("dataAddress", "'Data'!B6:C50") \
                                    .option("header", "false") \
                                    .schema(inflation_schema) \
                                    .load(inputs + "/inflation/" + countries[i]) \
                                    .withColumn("country_code", \
                                            functions.lit(country_code)) \
                                    .withColumn("country", \
                                            functions.lit(country_name))
                                    # Ignore ".xlsx" extension from country name
                                    # Need lit function to initialize
                                    # column w/ same value

        gdp_data = spark.read.format("com.crealytics.spark.excel") \
                            .option("dataAddress", "'Data'!B6:C50") \
                            .option("header", "false") \
                            .schema(gdp_schema) \
                            .load(inputs + "/gdp_per_capita/" + countries[i]) \
                            .withColumn("country_code", \
                                    functions.lit(country_code)) \
                            .withColumn("country", \
                                    functions.lit(country_name))
                            # Ignore ".xlsx" extension from country name
                            # Need lit function to initialize
                            # column w/ same value

        unemployment_data = spark.read.format("com.crealytics.spark.excel") \
                                    .option("dataAddress", "'Data'!B6:C50") \
                                    .option("header", "false") \
                                    .schema(unemployment_schema) \
                                    .load(inputs + "/unemployment/" + countries[i]) \
                                    .withColumn("country_code", \
                                            functions.lit(country_code)) \
                                    .withColumn("country", \
                                            functions.lit(country_name))
                                    # Ignore ".xlsx" extension from country name
                                    # Need lit function to initialize
                                    # column w/ same value

        # Combine current country stats to DataFrame of all country stats
        inflation_dataframe = inflation_dataframe.unionAll(inflation_data)
        gdp_dataframe = gdp_dataframe.unionAll(gdp_data)
        unemployment_dataframe = unemployment_dataframe.unionAll(unemployment_data)

    # Filter for data between 2018 and 2022
    inflation_filtered = filter_econ_data(inflation_dataframe)
    gdp_filtered = filter_econ_data(gdp_dataframe)
    unemployment_filtered = filter_econ_data(unemployment_dataframe)

    # Join results of inflation rate, GDP per capita,
    # and unemployment rate into a single DataFrame
    combined_dataframe = inflation_filtered.join(gdp_filtered, \
                            on=["year", "country_code", "country"])

    final_dataframe = combined_dataframe.join(unemployment_filtered, \
                            on=["year", "country_code", "country"])

    return final_dataframe

def main(inputs, output):
    # PART 1 - ECONOMIC DATA
    # Get all country file names
    # https://stackoverflow.com/a/3207254
    country_files = os.listdir(inputs + "/gdp_per_capita")

    # Get inflation rates, GDP, and unemployment rates
    # of our selected countries from 2018-2022
    econ_data = read_inflation_gdp(country_files)

    global_inf_schema = types.StructType([
        types.StructField("year", types.IntegerType()),
        types.StructField("global_inflation_rate", types.FloatType())
    ])

    global_une_schema = types.StructType([
        types.StructField("year", types.IntegerType()),
        types.StructField("global_unemployment_rate", types.FloatType())
    ])

    # Read files of global inflation and unemployment rates
    global_inflation = spark.read.format("com.crealytics.spark.excel") \
                                .option("dataAddress", "'Data'!B6:C50") \
                                .option("header", "false") \
                                .schema(global_inf_schema) \
                                .load(inputs + "/inflation/global.xlsx")

    global_unemployment = spark.read.format("com.crealytics.spark.excel") \
                                    .option("dataAddress", "'Data'!B6:C50") \
                                    .option("header", "false") \
                                    .schema(global_une_schema) \
                                    .load(inputs + "/unemployment/global.xlsx")

    # Add global inflation and unemployment data to our existing DataFrame
    # Final economic data will be used twice later, so cache
    combined_econ = econ_data.join(global_inflation, on="year")
    final_econ = combined_econ.join(global_unemployment, on="year").cache()

    # PART 2 - HAPPINESS DATA
    happiness_schema = types.StructType([
        types.StructField("country", types.StringType()),
        types.StructField("year", types.IntegerType()),
        types.StructField("happiness", types.FloatType())
    ])

    happiness = spark.read.format("com.crealytics.spark.excel") \
                            .option("dataAddress", "'Sheet1'!A2:C2367") \
                            .option("header", "false") \
                            .schema(happiness_schema) \
                            .load(inputs + "/happiness.xls")

    # Get list of distinct countries from econ data above,
    # as happiness data is all in one file
    # https://stackoverflow.com/a/47046277
    # https://stackoverflow.com/a/55376523
    # Can use collect because we're only working with 18 countries,
    # but even if we were to expand the number of countries,
    # the total number of countries in the world won't be too many
    distinct_countries = final_econ.select(final_econ["country"]).distinct()
    countries_list = [row["country"] for row in distinct_countries.collect()]

    # Filter for 2018-2022 year range, and use isin function
    # to grab only the columns with the countries we're working with
    # https://stackoverflow.com/a/46707814
    filtered_happiness = happiness.filter((happiness["year"] >= 2018) & \
                                        (happiness["year"] <= 2022) & \
                                        (happiness["country"].isin(countries_list)))

    # Merge happiness data with economic data
    econ_happiness = final_econ.join(filtered_happiness, on=["year", "country"])

    # Add column indicating whether country is developed or developing
    developed_countries = ["United Kingdom", "Japan", "Canada", "Singapore", \
                "Australia", "Norway", "Belgium", "Hong Kong", "United States"]

    country_development = econ_happiness.withColumn("developed_country", \
                            functions.when(functions.col("country") \
                            .isin(developed_countries), True).otherwise(False))

    # Keep relevant rows in a sensible order to write to Parquet
    final_econ_happiness = country_development[["year", "country", "country_code", \
                                                "developed_country", \
                                                "gdp_per_capita", "inflation_rate", \
                                                "unemployment_rate", "happiness", \
                                                "global_inflation_rate", \
                                                "global_unemployment_rate"]]

    final_econ_happiness.write.partitionBy("country_code") \
                        .parquet(output, mode="overwrite")

# Sample command to run this code:
# $ spark-submit --packages com.crealytics:spark-excel_2.12:0.13.5
# econ_happiness_etl.py ../pre-etl_data ../cleaned_data/econ_happiness_cleaned
# Specify crealytics in packages, as per
# https://stackoverflow.com/a/68206031
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Economic and happiness data ETL') \
            .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
            .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)