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

sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
#SparkFiles.get("file:///home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
import cleaning_io as clean

def delete_useless_column(attrF, df, rate):
	idxDel = [i for i, j in enumerate(attrF) if j > rate]
	attrDel = [df.columns[i] for i in idxDel]
	for attr in attrDel:
		df = df.drop(attr)
	return df

def process(filename, output):
	dt = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
	# Change name of the header (fill in black space with ‘_’ and delete ‘()’)
	df2 = clean.fixColumnName(dt)
	# Finding Null value (if the attribute has 90% null value, delete the attribute)
	df2 = clean.deleteNullAttr(df2)
	# Date Transformation
	if ("weather" in filename):
		# weather data
		df2 = df.withColumn('date', col('date').cast(StringType()))
		df2 = df2.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'yyyymmdd')))).drop('date')
	elif ("311" in filename):
		df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('created_date', 'MM/dd/yyyy'))))
		df2 = df2.withColumn('time', col('created_date').substr(12, 11)).drop("created_date")
	elif ("property" in filename):
		df2 = df.withColumn("yymmdd", to_date(from_unixtime(unix_timestamp('year', 'yyyy/mm')))).drop('year')
	else:
		df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'MM/dd/yyyy')))).drop('date')
	# Map time (HH:MM) into 24 hours
	if ("311" in filename):
		df2 = df2.withColumn('time24', when(split(col("time"), " ")[1]=="AM", split(col("time"), ':')[0].cast("int")).otherwise(split(col("time"), ':')[0].cast("int")+12))
		df2 = df2.drop("time")
	elif ("weather" in filename):
		df2 = df2.withColumn('time24', floor(df2.time/100) ).drop('time')
	elif ("property" not in filename):
		df2 = df2.withColumn('time24',split(col("time"), ':')[0].cast("int")).drop('time')
	# Date into day only
	if ("property" not in filename):
		df2 = df2.withColumn('day', dayofmonth("yymmdd"))
	# Date into month only
	df2 = df2.withColumn('month', month("yymmdd"))
	# Date into year only
	df2 = df2.withColumn('year', year("yymmdd"))
	# Additional file cleaning (file specific)
	if ("weather" in filename):
		# drop column with 90% 999 999.99 ..etc useless rows
		df2 = df2.drop('sd','sdw','sa')
		df2 = df2.withColumn('spd_floor',floor(df2.spd)).drop('spd')
		df2 = df2.withColumn('temp_floor',floor(df2.temp)).drop('temp')
		df2 = df2.withColumn('prcp_floor',floor(df2.prcp)).drop('prcp')
		df2 = df2.filter(df2.visb != '      ')
	if ("property" in filename):
		# ['bble', 'b', 'block', 'lot', 'owner', 'bldgcl', 'taxclass', 'ltfront', 'ltdepth', 'ext', 'stories', 'fullval', 'avland', 'avtot', 'exland', 'extot',
		 # 'excd1', 'staddr', 'zip', 'bldfront', 'blddepth', 'avland2', 'avtot2', 'extot2', 'period', 'year', 'valtype', 'yymmdd', 'month']
		# Delete attrs w/o description
		df2 = df2.drop('lot')
		# Delete attrs with too many categories
		df2 = df2.drop('block', 'owner','staddr')
		# Delete attrs that most data has the same category value
		df2 = df2.drop('valtype', 'period')
		df2 = df2.withColumn('stories_floor', floor(df2.stories)).drop('stories')	
		df2 = df2.withColumn('excd1_floor', floor(df2.excd1)).drop('excd1')
		df2 = df2.withColumn('unique_key', df2.bble).drop('bble')
		df2 = df2.withColumn('ltDepth', floor(df2.ltdepth / 100)).drop('ltdepth')
		df2 = df2.withColumn('ltFront', floor(df2.ltfront / 100)).drop('ltfront')
		df2 = df2.withColumn('fullVal', floor(df2.fullval / 1000000)).drop('fullval')
		df2 = df2.withColumn('avland_floor', floor(df2.avland / 1000000)).drop('avland')
		df2 = df2.withColumn('avtot_floor', floor(df2.avtot / 1000000)).drop('avtot')
		df2 = df2.withColumn('exland_floor', floor(df2.exland / 1000000)).drop('exland')
		df2 = df2.withColumn('extot_floor', floor(df2.extot / 1000000)).drop('extot')
		df2 = df2.withColumn('avland2_floor', floor(df2.avland2 / 1000000)).drop('avland2')
		df2 = df2.withColumn('avtot2_floor', floor(df2.avtot2 / 1000000)).drop('avtot2')
		df2 = df2.withColumn('extot2_floor', floor(df2.extot2 / 1000000)).drop('extot2')
	df2.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save(output)


spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()
	
##################################
# cleaning : taxi
##################################
folder = "/user/jn1664/"
year = 2009
month = 1
filename = folder + str(year) + "-"
if (month < 10):
	filename = filename + "0"

filename = filename + str(month) + "_startGPS.txt"

##################################
# cleaning : property
##################################
folder = "file:///home/hk2451/project/data/property/avroll_"
for yearIdx in range(10, 18):
	print(yearIdx)
	filename = folder + str(yearIdx) + ".csv"
	output = "property_cleaned" + str(yearIdx)
	process(filename, output)

spark.sql("SELECT * FROM DT").coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save(output)
