#!/usr/bin/env python

import sys
from csv import reader 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col
from pyspark.sql.types import DateType,StringType
from datetime import datetime

def delete_useless_column(attrF, df, rate):
	idxDel = [i for i, j in enumerate(attrF) if j > rate]
	attrDel = [df.columns[i] for i in idxDel]
	for attr in attrDel:
		df = df.drop(attr)
	return df

spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()
# dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/collision/NYPD_Motor_Vehicle_Collisions.csv")

filename = "file:///home/hk2451/project/data/weather/weather-2011-2017.csv"
dt = spark.read.format('csv').options(header='true',inferschema='true').load(filename)

# Change name of the header (fill in black space with ‘_’ and delete ‘()’)
exprs0 = [col(column).alias(column.strip()) for column in dt.columns]
df = dt.select(*exprs0)
exprs1 = [col(column).alias(column.replace(' ', '_')) for column in dt.columns]
df = df.select(*exprs1)
exprs2 = [col(column).alias(column.replace('(', '')) for column in df.columns]
df = df.select(*exprs2)
exprs3 = [col(column).alias(column.replace(')', '')) for column in df.columns]
df = df.select(*exprs3)
df = df.toDF(*[c.lower() for c in df.columns])


# Finding Null value (if the attribute has 90% null value, delete the attribute)
totL = df.count()
attrF = []
for colName in df.columns:
	attrF.append(df.filter(str(colName)+" is null").count()/totL)

df = delete_useless_column(attrF, df, 0.9)

# Finding Unspecified (if the attribute has over 90% unspecified, will delete)
attrF = []
for colName in df.columns:
	attrF.append(df.filter(df[colName].like("%Unspecified%")).count()/totL)

df = delete_useless_column(attrF, df, 0.9)

# Date Transformation
if ("weather" in filename):
	# weather data
	df2 = df.withColumn('date', col('date').cast(StringType()))
	df2 = df2.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'yyyymmdd')))).drop('date')

else if ("311" in filename):
	df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('created_date', 'MM/dd/yyyy'))))
	df2 = df2.withColumn('time', col('created_date').substr(12, 11)).drop("created_date")

else if ("property" in filename):
	df2 = df.withColumn("yymmdd", to_date(from_unixtime(unix_timestamp('year', 'yyyy/mm'))))

else:
	df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'MM/dd/yyyy')))).drop('date')

# Map time (HH:MM) into 24 hours
if ("311" in filename):
	df2 = df2.withColumn('time24', when(split(col("time"), " ")[1]=="AM", split(col("time"), ':')[0].cast("int")).otherwise(split(col("time"), ':')[0].cast("int")+12))
	df2 = df2.drop("time")

else if ("property" not in filename):
	df2 = df2.withColumn('time24',split(col("time"), ':')[0].cast("int")).drop('time')


# Date into day only
if ("property" not in filename):
	df2 = df2.withColumn('day', dayofmonth("yymmdd"))

# Date into month only
df2 = df2.withColumn('month', month("yymmdd"))

# Date into year only
df2 = df2.withColumn('year', year("yymmdd"))

df2.createOrReplaceTempView("DT")

spark.sql("SELECT * FROM DT").coalesce(1).write.save("Weather_Cleaned", format="csv")

