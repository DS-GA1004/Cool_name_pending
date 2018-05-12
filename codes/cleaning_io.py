#!/usr/bin/env python

# - import common modules
import sys
import numpy as np
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col
from datetime import datetime


spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()

def fixColumnName(dt):
	# Input: 
	#  - dt : dataframe	
	
	# Changing attribute (column) names
	exprs = [col(column).alias(column.replace(' ', '_')) for column in dt.columns]
	df = dt.select(*exprs)
	exprs2 = [col(column).alias(column.replace('(', '')) for column in df.columns]
	df = df.select(*exprs2)
	exprs3 = [col(column).alias(column.replace(')', '')) for column in df.columns]
	df = df.select(*exprs3)
	df = df.toDF(*[c.lower() for c in df.columns])
	return df

def deleteNullAttr(dt):
	# Input: 
	#  - df : dataframe	
	
	# Deleting attribute (column) if has large amount of null or unspecified data 
	totL = dt.count()
	attrFn = []
	attrFu = []
	
	for colName in dt.columns:
		attrFn.append(dt.filter(str(colName)+" is null").count()/totL)
		attrFu.append(dt.filter(dt[colName].like("%Unspecified%")).count()/totL)
	
	attrIdx = list(set().union([x[0] for x in enumerate(attrFn) if x[1] > 0.9],[x[0] for x in enumerate(attrFu) if x[1] > 0.9]))
	#df = deleteColumn(attrIdx, dt)
	attrDel = [dt.columns[i] for i in attrIdx]
	for attr in attrDel:
		dt = dt.drop(attr)
	return dt


def zipcodeFilter(dt, zipAttr):
	# Input: 
	#  - df : dataframe	
	#  - zipAttr : string name of zip code attribute (column)
	df = dt.withColumn('zipcode', substring(dt[zipAttr], 1, 5)).drop(zipAttr)
	nyc_zips = spark.read.format('csv').options(header='true', inferschema='true').load("file:///home/hk2451/project/Cool_name_pending/codes/cleaning/nyc_zip_code.csv")
	df.createOrReplaceTempView("whole")
	nyc_zips.createOrReplaceTempView("nyc_zips")
	df2 = spark.sql("SELECT * FROM whole WHERE whole.zipcode in (SELECT zipcode FROM nyc_zips)")
	return df2




