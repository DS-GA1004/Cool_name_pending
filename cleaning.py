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
dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/311/311_Service_Requests_from_2011.csv")

# Change name of the header (fill in black space with ‘_’ and delete ‘()’)
exprs = [col(column).alias(column.replace(' ', '_')) for column in dt.columns]
df = dt.select(*exprs)
exprs2 = [col(column).alias(column.replace('(', '')) for column in df.columns]
df = df.select(*exprs2)
exprs3 = [col(column).alias(column.replace(')', '')) for column in df.columns]
df = df.select(*exprs3)

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
#df2 = df.select('Created_Date', to_date(from_unixtime(unix_timestamp('Created_Date', 'MM/dd/yyy'))).alias('date'))
df2 = df.withColumn('date', to_date(from_unixtime(unix_timestamp('Created_Date', 'MM/dd/yyy')))).drop("Created_Date")

df2.createOrReplaceTempView("DT")

CT = spark.sql("SELECT Complaint_Type, date, COUNT(Complaint_Type) as counter FROM DT GROUP BY Complaint_Type, date")

# Delete Row w/o legal position
df3 = spark.sql("SELECT * From DT a Where (a.INcident_Zip NOT Like '%None%' AND a.INcident_Zip NOT Like '%Unspecified%') \
OR (a.School_Zip NOT Like '%None%' AND a.School_Zip NOT Like '%Unspecified%') \
OR (a.Latitude NOT Like '%None%' AND a.Latitude NOT Like '%Unspecified%' ) \
OR (a.Longitude NOT Like '%None%' AND a.Longitude NOT Like '%Unspecified%' ) \
OR (a.Location NOT Like '%None%' AND a.Location NOT Like '%Unspecified%')")

