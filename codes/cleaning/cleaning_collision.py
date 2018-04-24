#!/usr/bin/env python

import sys
from csv import reader 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col
from datetime import datetime

def delete_useless_column(attrF, df, rate):
	idxDel = [i for i, j in enumerate(attrF) if j > rate]
	attrDel = [df.columns[i] for i in idxDel]
	for attr in attrDel:
		df = df.drop(attr)
	return df

spark = SparkSession.builder.appName("Python Spark SQL task1").config("spark.some.config.option", "some-value").getOrCreate()
dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/collision/NYPD_Motor_Vehicle_Collisions.csv")

# Change name of the header (fill in black space with ‘_’ and delete ‘()’)
exprs = [col(column).alias(column.replace(' ', '_')) for column in dt.columns]
df = dt.select(*exprs)
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

# delete high-variance columns
attrF = []
for colName in df.columns:
	x = df.select(colName).distinct()
	attrF.append(x.count() / totL)
df = delete_useless_column(attrF, df, 0.75)

# Finding Unspecified (if the attribute has over 90% unspecified, will delete)
attrF = []
for colName in df.columns:
	attrF.append(df.filter(df[colName].like("%Unspecified%")).count()/totL)
df = delete_useless_column(attrF, df, 0.9)

# Date Transformation
df2 = df.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'MM/dd/yyy')))).drop('date')

# Map time (HH:MM) into 24 hours
df2 = df2.withColumn('time24',split(col("time"), ':')[0]).drop('time')

# Date into day only
df2 = df2.withColumn('day', day("yymmdd"))

# Date into month only
df2 = df2.withColumn('month', month("yymmdd"))

# Date into year only
df2 = df2.withColumn('year', year("yymmdd"))

df2.createOrReplaceTempView("DT")


