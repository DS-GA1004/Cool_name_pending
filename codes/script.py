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
SparkFiles.get("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
#sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
import cleaning_io as clean

dt1 = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/311/311_Service_Requests_from_2010_to_Present.csv")
dt2 = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/data/collision/NYPD_Motor_Vehicle_Collisions.csv")

##################################
# cleaning : complaint 
##################################
df1 = clean.fixColumnName(dt1)
df1 = clean.deleteNullAttr(df1)
df1 = clean.zipcodeFilter(df1, 'incident_zip')
df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('created_date', 'MM/dd/yyyy'))))
df1 = df1.withColumn('time', col('created_date').substr(12, 11)).drop("created_date")
df1 = df1.withColumn('time24', when(split(col("time"), " ")[1]=="AM", split(col("time"), ':')[0].cast("int")).otherwise(split(col("time"), ':')[0].cast("int")+12))
df1 = df1.drop("time")
# Unique key is same as number or lines -> deleted
df1 = df1.drop('unique_key')
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
df2 = clean.dateTranform(df2, 'date')
df2 = clean.zipcodeFilter(df2, 'zip_code')
df2 = clean.timeTranform(df2, 'time')
# Unique key is same as number or lines -> deleted
df2 = df2.drop('unique_key')
# Since we have zip code and address:
# # latitude, longitude,etc address related attributes -> deleted
df2 = df2.drop('longitude','latitude','location','cross_street_name','off_street_name')

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
	dfJoin = dfJoin.drop(colab,colab+'A')

# Select year
dfJoinF12 = dfJoin.filter(col('year') == 2012)
dfJoinF12.count()

dfJoinF12.createOrReplaceTempView("DT")
spark.sql("SELECT * FROM DT").coalesce(1).write.save("file:///home/hk2451/project/data/join_year_2012", format="csv")

spark.sql("SELECT * FROM DT").coalesce(1).write.save("join_year_2012", format="csv")







