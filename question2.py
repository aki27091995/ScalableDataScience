from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

schema_daily = StructType([
    StructField('ID', StringType()),
    StructField('DATE', DateType()),
    StructField('ELEMENT', StringType()),
    StructField('VALUE', IntegerType()),
    StructField('MEASUREMENT FLAG', StringType()),
    StructField('QUALITY FLAG', StringType()),
    StructField('SOURCE FLAG', StringType()),
    StructField('OBSERVATION TIME', StringType()),
])

daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("dateFormat", "yyyyMMdd")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(1000, False)


schema_stations = StructType([
    StructField('ID', StringType()),
    StructField('LATITUDE', DoubleType()),
    StructField('LONGITUDE', DoubleType()),
    StructField('ELEVATION', DoubleType()),
    StructField('STATE', StringType()),
    StructField('NAME', StringType()),
    StructField('GSN FLAG', StringType()),
    StructField('HCN/CRN FLAG', StringType()),
    StructField('WMO ID', StringType()),
])

stations = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_stations)
    .load("hdfs:///data/ghcnd/stations")
    .limit(1000)
)

schema_countries = StructType([
    StructField('CODE', StringType()),
    StructField('NAME', StringType()),
])

countries = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_stations)
    .load("hdfs:///data/ghcnd/countries")
    .limit(1000)
)

schema_states = ([
    StructField('CODE', StringType()),
    StructField('NAME', StringType()),
])

states = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_stations)
    .load("hdfs:///data/ghcnd/states")
    .limit(1000)
)

schema_inventory = ([
    StructField('ID', StringType()),
    StructField('LATITUDE', DoubleType()),
    StructField('LONGITUDE', DoubleType()),
    StructField('ELEMENT', StringType()),
    StructField('FIRSTYEAR', IntegerType()),
    StructField('LASTYEAR', IntegerType()),
])

inventory = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_stations)
    .load("hdfs:///data/ghcnd/inventory")
    .limit(1000)
)
   
