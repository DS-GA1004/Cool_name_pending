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

def process(filename, output):
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
	elif ("311" in filename):
		df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('created_date', 'MM/dd/yyyy'))))
		df2 = df2.withColumn('time', col('created_date').substr(12, 11)).drop("created_date")
	elif ("property" in filename):
		df2 = df.withColumn("yymmdd", to_date(from_unixtime(unix_timestamp('year', 'yyyy/mm'))))
	else:
		df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'MM/dd/yyyy')))).drop('date')
	# Delete columns w/o description
	if ("property" in filename):
		# Delete attrs with too many categories
		df2 = df2.drop('block', 'lot', 'ltdepth', 'owner', 'ltfront', 'ltdep', 'fullval', 'AVLAND', 'AVTOT', 'EXLAND', 'EXTOT', 'staddr', 'avland2', 'avtot2', 'extot2', 'year')
		# Delete attrs that most data has the same category value
		df2 = df2.drop('valtype', 'period')
		df2 = df2.withColumn('stories_floor', floor(df2.stories)).drop('stories')
		df2 = df2.withColumn('excd1_floor', floor(df2.excd1)).drop('excd1')
		df2 = df2.withColumn('unique_key', df2.bble).drop('bble')
	# Map time (HH:MM) into 24 hours
	if ("311" in filename):
		df2 = df2.withColumn('time24', when(split(col("time"), " ")[1]=="AM", split(col("time"), ':')[0].cast("int")).otherwise(split(col("time"), ':')[0].cast("int")+12))
		df2 = df2.drop("time")
	elif ("property" not in filename):
		df2 = df2.withColumn('time24',split(col("time"), ':')[0].cast("int")).drop('time')
	# Date into day only
	if ("property" not in filename):
		df2 = df2.withColumn('day', dayofmonth("yymmdd"))
	# Date into month only
	df2 = df2.withColumn('month', month("yymmdd"))
	# Date into year only
	df2 = df2.withColumn('year', year("yymmdd"))
	df2.createOrReplaceTempView("DT")
	spark.sql("SELECT * FROM DT").coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save(output)

spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()
# dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/collision/NYPD_Motor_Vehicle_Collisions.csv")

folder = "file:///home/hk2451/project/data/property/avroll_"
for i in range(10, 18):
	filename = folder + str(i) + ".csv"
	process(filename, "property"+str(i))
# filename = folder + "10.csv"
# df2 = process(filename, "property")
# spark.sql("SELECT * FROM DT").coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save(output)
