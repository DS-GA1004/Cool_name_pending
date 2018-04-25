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

def deleteColumn(df,attrIdx):
	# Input: 
	#  - df : dataframe	
	#  - attrIdx : attribute (column) index from dataframe
	
	# Deleting attribute based on attribute index
	attrDel = [df.columns[i] for i in attrIdx]
	for attr in attrDel:
		df = df.drop(attr)
	return df

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
	df = deleteColumn(attrIdx, dt)
	return df

def dateTranform(dt, dateAttr):
	# Input: 
	#  - df : dataframe	
	#  - dateAttr : string name of date attribute (column)

	# Date into date format
	dt = dt.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp(dateAttr, 'MM/dd/yyy')))).drop(dateAttr)
	# Date into day only
	dt = dt.withColumn('day', dayofmonth("yymmdd"))
	# Date into month only
	dt = dt.withColumn('month', month("yymmdd"))
	# Date into year only
	dt = dt.withColumn('year', year("yymmdd"))
	return dt

def timeTranform(dt, timeAttr):
	# Input: 
	#  - df : dataframe	
	#  - timeAttr : string name of time attribute (column)

	# Map time (HH:MM) into 24 hours
	dt = dt.withColumn('time24',split(col(timeAttr), ':')[0].cast("int")).drop(timeAttr)
	return dt




