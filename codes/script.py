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
from pyspark import SparkFiles
import datetime

sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
sc.addFile("/home/hk2451/project/Cool_name_pending/codes/mutual_information.py")
#SparkFiles.get("file:///home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
import cleaning_io as clean
import mutual_information as mi

dt1 = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/311/311_Service_Requests_from_2010_to_Present.csv")
dt2 = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/collision/NYPD_Motor_Vehicle_Collisions.csv")

# cleanJoinF12 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/join_year_2012_2")


##################################
# cleaning : complaint 
##################################
df1 = clean.fixColumnName(dt1)
df1 = clean.deleteNullAttr(df1)
df1 = clean.zipcodeFilter(df1, 'incident_zip')
df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('created_date', 'MM/dd/yyyy'))))
# Date into day only
df1 = df1.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
df1 = df1.withColumn('month', month("yymmdd"))
# Date into year only
df1 = df1.withColumn('year', year("yymmdd"))
df1 = df1.withColumn('time', col('created_date').substr(12, 11)).drop("created_date")
df1 = df1.withColumn('time24', when(split(col("time"), " ")[1]=="AM", split(col("time"), ':')[0].cast("int")).otherwise(split(col("time"), ':')[0].cast("int")+12))
df1 = df1.drop("time")
# Unique key is same as number or lines -> deleted
#df1 = df1.drop('unique_key')
# Since we have ***Street Name***:
# # Cross Street1,2, Intersection Street1,2, ***incident address***-> deleted 
df1 = df1.drop('cross_street_1','cross_street_2','intersection_street_1','intersection_street_2','incident_address')
# Since we have zip code and address:
# # x_coordinate_state_plane, longitude,etc address related attributes -> deleted
df1 = df1.drop('x_coordinate_state_plane','y_coordinate_state_plane','longitude','latitude','location','community_board')
# park borough is same as borough -> deleted
df1 = df1.drop('park_borough')
# No discription about bbl -> deleted
df1 = df1.drop('bbl')
# agency is abbribiated version of agency name -> deleted
df1 = df1.drop('agency_name')
# Filled in sentences -> deleted 
df1 = df1.drop('resolution_description')
# For dates, we only considered "when complaints happend"  -> deleted
df1 = df1.drop('closed_date','resolution_action_updated_date','due_date')
# Complaint type has 237 distinct categories. Desciptor is a detailed version of complaint type, which has 1126 categories. -> deleted
df1 = df1.drop('descriptor')

##################################
# cleaning : collision 
##################################
df2 = clean.fixColumnName(dt2)
df2 = clean.deleteNullAttr(df2)
df2 = df2.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('date', 'MM/dd/yyy')))).drop('date')
# Date into day only
df2 = df2.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
df2 = df2.withColumn('month', month("yymmdd"))
# Date into year only
df2 = df2.withColumn('year', year("yymmdd"))
df2 = clean.dateTranform(df2, 'date')
df2 = clean.zipcodeFilter(df2, 'zip_code')
df2 = df2.withColumn('time24',split(col('time'), ':')[0].cast("int")).drop('time')
# Unique key is same as number or lines -> deleted
#df2 = df2.drop('unique_key')
# Since we have zip code and address:
# # latitude, longitude,etc address related attributes -> deleted
df2 = df2.drop('longitude','latitude','location','cross_street_name','off_street_name')

df2.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("collision_cleaned")

##################################
# cleaning : weather & property
##################################
# Change the filename below
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

if ("weather" in filename):
	df2 = df2.withColumn('time24', floor(df2.time/100) ).drop('time')
	
else if ("property" not in filename):
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

df2.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("weather_cleaned")


##################################
# cleaning : taxi
##################################
dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/taxi/yellow_tripdata_2017-01.csv")
# Trip duration
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
dt = dt.withColumn('tripTime',unix_timestamp(dt.tpep_dropoff_datetime,format=timeFmt)-unix_timestamp(dt.tpep_pickup_datetime,format=timeFmt))
dt = dt.drop('tpep_dropoff_datetime')

# Pickup datetime as date / time
dt = dt.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('tpep_pickup_datetime', 'MM/dd/yyyy'))))
# Day only
dt = dt.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
dt = dt.withColumn('month', month("yymmdd"))
# Date into year only
dt = dt.withColumn('year', year("yymmdd"))
# Time in 24 
dt = dt.withColumn('time24', date_format(from_unixtime(unix_timestamp('tpep_pickup_datetime')), 'HH')).drop('tpep_pickup_datetime')


##################################
# Inner Join and filter by year
##################################
# find common name column and change it 

colA = df1.columns
colB = df2.columns
colAB = list(set(colA).intersection(colB))

for colab in colAB:
	df1 = df1.withColumnRenamed(colab,colab+'A')

a = df1.alias('a')
b = df2.alias('b')

dfJoin = a.join(b, a.yymmddA == b.yymmdd, 'inner').select([col('a.'+xx) for xx in a.columns] + [col('b.'+xx) for xx in b.columns])
# Delete duplicated columns

for colab in colAB:
	dfJoin = dfJoin.drop(colab+'A')

# Select year
dfJoinF12 = dfJoin.filter(col('year') == 2012)
dfJoinF12.count()

dfJoinF12.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("join_year_2012_2")

##################################
# Mutual Information
##################################

attr1 = 'agency'
attr2 = 'number_of_persons_injured'
uniqueAttr = 'unique_key'
saveFlag = 1
saveName = 'mutual_trial'

# If you want to save file
MI = mi.mutual_information(dt, attr1, attr2, uniqueAttr,saveFlag,saveName)

# If you don't need to save it
MI = mi.mutual_information(dt, attr1, attr2, uniqueAttr,0,saveName)






