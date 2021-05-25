import os, re
import configparser
import pandas as pd
from datetime import timedelta, datetime
from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType

def create_spark_session():
    """
    Create session with Spark
    """

    #Set env variables to handle different errors regarding Udacity workspace
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark

def process_immigration(spark, input_path, output_path, imput_format = 'csv'):
    """
    Read the immigration dataset. Perform ETL processes using spark - clean and structure data for futher analysis. Then save data in parquet format.

    Args:
    spark (:obj:`SparkSession`): Spark session. 
    input_path (:obj:`str`): Path to file with data to be loaded. 
    output_path (:obj:`str`): Path for output files.
    input_format (:obj:`str`): Type of the input files. Default to "csv".
    """
        
    #Load immigration data and rename default columns
    immigration = spark.read.load(input_path, format=imput_format, header=True).select('*')\
        .withColumnRenamed('cicid','ID')\
        .withColumnRenamed('i94yr','year')\
        .withColumnRenamed('i94mon','month')\
        .withColumnRenamed('i94port','port_of_admission')\
        .withColumnRenamed('i94mode','mode_of_trans')\
        .withColumnRenamed('i94addr','arrival_state')\
        .withColumnRenamed('i94visa','visa_code')\
        .withColumnRenamed('biryear','birth_year')\
        .withColumnRenamed('i94bir','birth_city')
    
    #Drop not useful columns
    not_useful = ["count", "entdepa", "entdepd", "matflag", "fltno", "dtaddto", "admnum", "i94bir", "dtadfile", "visapost", "occup", "entdepu", "insnum"]
    immigration = immigration.drop(*not_useful)

    # drop null records from i94addr column
    immigration = immigration.na.drop(subset=["arrival_state"])
    #Change I94MODE code to string
    transport_lookup = spark.read.load("lookup/I94MODE.csv", format="csv", columns="*", header=True).select('*').withColumnRenamed('ID', 'ID_TMP')
    immigration = immigration.withColumn("mode_of_trans",col("mode_of_trans").cast("int"))
    immigration = immigration.join(transport_lookup, transport_lookup["ID_TMP"] == immigration['mode_of_trans'], "left")
    immigration = immigration.drop('ID_TMP', 'mode_of_trans')

    #Cast to int
    immigration = immigration.withColumn("year",col("year").cast("int"))\
        .withColumn("month",col("month").cast("int"))\
        .withColumn("ID",col("ID").cast("int"))\
        .withColumn("birth_year",col("birth_year").cast("int"))

    #Decode I94VISA
    visa_lookup = spark.read.load("lookup/I94VISA.csv", format="csv", columns="*", header=True).select('*').withColumnRenamed('ID', 'ID_TMP')\
    .withColumnRenamed('Type', 'visa')
    immigration = immigration.withColumn("visa_code",col("visa_code").cast("int"))
    immigration = immigration.join(visa_lookup, visa_lookup["ID_TMP"] == immigration['visa_code'], "left")
    immigration = immigration.drop('ID_TMP', 'visa_code')

    #Convert SAS datatype to dataframe
    immigration = immigration.withColumn("base_sas_date", to_date(lit("01/01/1960"), "MM/dd/yyyy")) \
                .withColumn("arrival_date", expr("date_add(base_sas_date, arrdate)")) \
                .withColumn("departure_date", expr("date_add(base_sas_date, depdate)")) \
                .drop("base_sas_date", "arrdate", "depdate")

    #Decode I94CITY
    city_lookup = spark.read.load("lookup/I94CIT_I94RES.csv", format="csv", columns="*", header=True).select('*')
    immigration = immigration.withColumn("i94cit",col("i94cit").cast("int"))\
        .withColumn("i94res",col("i94res").cast("int"))

    immigration = immigration.join(city_lookup, city_lookup["Code"] == immigration['i94cit'], "left")\
        .withColumnRenamed('I94CTRY', 'native_country')\
        .drop('i94cit', 'Code')

    immigration = immigration.join(city_lookup, city_lookup["Code"] == immigration['i94res'], "left")\
        .withColumnRenamed('I94CTRY', 'residence_country')\
        .drop('i94res', 'Code')
    
     #Save the DataFrame to data source
    immigration.select('*').write.save(output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None)
    
    
def process_demographics(spark, input_path, output_path, imput_format = 'csv'):
    """
    Read the demographics dataset. Perform ETL processes using spark - clean and structure data for futher analysis. Then save data in parquet format.

    Args:
    spark (:obj:`SparkSession`): Spark session. 
    input_path (:obj:`str`): Path to file with data to be loaded. 
    output_path (:obj:`str`): Path for output files.
    input_format (:obj:`str`): Type of the input files. Default to "csv".
    """
    
    #Load data, rename columns and cast several float typed columns to int
    demographics = spark.read.load(input_path, format=imput_format, sep=';', header=True).select('*')\
        .withColumnRenamed('Median Age', 'Median_age')\
        .withColumnRenamed('Male Population', 'Male_population')\
        .withColumnRenamed('Female Population', 'Female_population')\
        .withColumnRenamed('Total Population', 'Total_population')\
        .withColumnRenamed('Number of Veterans', 'Number_of_veterans')\
        .withColumnRenamed('Average Household Size', 'Average_household_size')\
        .withColumnRenamed('State Code', 'State_code')\
        .withColumn("Male_population",col("Male_population").cast("int"))\
        .withColumn("Female_population",col("Female_population").cast("int"))\
        .withColumn("Total_population",col("Total_population").cast("int"))\
        .withColumn("Number_of_veterans",col("Number_of_veterans").cast("int"))\
        .withColumn("Foreign-born",col("Foreign-born").cast("int"))\
        .withColumn("Count",col("Count").cast("int"))

    #Drop null values
    demographics = demographics.na.drop(subset=['Male_population', 'Female_population', 'Number_of_veterans', 'Foreign-born', 'Average_household_size' ])
    
    #Save the DataFrame to data source
    demographics.select('*').write.save(output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None)

def process_temperature(spark, input_path, output_path, imput_format = 'csv'):
    """
    Read the temperature dataset. Perform ETL processes using spark - clean and structure data for futher analysis. Then save data in parquet format.

    Args:
    spark (:obj:`SparkSession`): Spark session. 
    input_path (:obj:`str`): Path to file with data to be loaded. 
    output_path (:obj:`str`): Path for output files.
    input_format (:obj:`str`): Type of the input files. Default to "csv".
    """
    
    #Load data to DataFrame
    temp = spark.read.load(input_path, format=imput_format, header=True).select('*')

    #Get avg temp by country
    temp = temp.groupby(["Country"]).agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
        .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
        .withColumnRenamed('first(Latitude)', 'Latitude')\
        .withColumnRenamed('first(Longitude)', 'Longitude')

    #make country caps
    temp = temp.withColumn("Country",upper(col("Country")))
    
    #Save the DataFrame to data source
    temp.select('*').write.save(output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None)

def process_airports(spark, input_path, output_path, imput_format = 'csv'):
    """
    Read the airports dataset. Perform ETL processes using spark - clean and structure data for futher analysis. Then save data in parquet format.

    Args:
    spark (:obj:`SparkSession`): Spark session. 
    input_path (:obj:`str`): Path to file with data to be loaded. 
    output_path (:obj:`str`): Path for output files.
    input_format (:obj:`str`): Type of the input files. Default to "csv".
    """
    
    #Load data to dataframe
    airports = spark.read.load(input_path, format=imput_format, header=True).select('*')
    
    #Keep only US related records
    airports = airports.filter((airports.iso_country == 'US'))
    
    #Remove US- from iso_region to get US State
    airports = airports.withColumn('iso_region', col('iso_region').substr(4,10)).withColumnRenamed('iso_region', 'state')
    
    #Save the DataFrame to data source
    airports.select('*').write.save(output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None)

if __name__ == "__main__" :
    
    # Load env variables located in "dl.cfg"
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    OUTPUT = config['ETL']['OUTPUT_DATA']
    
    # Create spark session
    spark = create_spark_session()
    
    '''
    For presentation purposes sample dataset was used as in Udacity workspace currently 
    is not possible to use SAS file because of missing SAS related Spark JAR package. 
    It's not possible to install this package at the moment, however application should work the same
    with SAS immigration file, just need to pass proper input_path and format.
    '''
    print('Process immigration')
    process_immigration(spark, input_path='immigration_data_sample.csv', output_path=OUTPUT + "immigration.parquet")
    
    print('Process demographics')
    process_demographics(spark, input_path='us-cities-demographics.csv', output_path=OUTPUT + "demographics.parquet")
    
    print('Process temperature')
    process_temperature(spark, input_path='../../data2/GlobalLandTemperaturesByCity.csv', output_path=OUTPUT + "temperature.parquet")
    
    print('Process airports')
    process_airports(spark, input_path='airport-codes_csv.csv', output_path=OUTPUT + "airports.parquet")