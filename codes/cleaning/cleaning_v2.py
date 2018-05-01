#!/usr/bin/env python

import sys
from csv import reader 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col
from datetime import datetime

spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()
dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/jy50/311_Service_Requests_from_2011_Cleaned.csv")
# dt = sc.textFile("file:///home/jy50/311_Service_Requests_from_2011_Cleaned.csv", 1)

# Keep First 5 digit
zipcode_field = 'Incident_Zip'
df1 = dt.withColumn('zipcode', substring(dt[zipcode_filed], 1, 5)).drop(zipcode_filed)

# Keep NYC Zipcode Only
nyc_zips = spark.read.format('csv').options(header='true', inferschema='true').load("file:///home/jy50/project/nyc_zip_code.csv")
df1.createOrReplaceTempView("whole")
nyc_zips.createOrReplaceTempView("nyc_zips")

df2 = spark.sql("SELECT * FROM whole WHERE whole.zipcode in (SELECT zipcode FROM nyc_zips)")
