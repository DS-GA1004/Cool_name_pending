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
filename = "file:///home/hk2451/project/data/property/avroll_10.csv"
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


df2.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("weather_cleaned")


##################################
# cleaning : taxi
##################################
yearIdx = 2017

folder = "file:///home/hk2451/project/data/taxi/yellow_tripdata_"
dt1 = spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-01.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-02.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-03.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-04.csv"))
dt2 = spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-05.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-06.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-07.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-08.csv"))
dt3 = spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-09.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-10.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-11.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load(folder+str(yearIdx)+"-12.csv"))

dt = dt1.union(dt2).union(dt3)

df1 = clean.fixColumnName(dt)
#df1 = clean.deleteNullAttr(df1)
df1 = clean.zipcodeFilter(df1, 'start_zip')
df1 = df1.withColumn('startZipcode',df1.zipcode).drop('zipcode')
df1 = clean.zipcodeFilter(df1, 'end_zip')
df1 = df1.withColumn('endZipcode',df1.zipcode).drop('zipcode')

# Trip duration (By minitues and categorized by every 10 min)
if yearIdx in [2017,2015,2016]:
	timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
	df1 = df1.withColumn('tripTime',floor((unix_timestamp(df1.tpep_dropoff_datetime,format=timeFmt)-unix_timestamp(df1.tpep_pickup_datetime,format=timeFmt))/600))
	df1 = df1.drop('tpep_dropoff_datetime')
else:
	timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
	df1 = df1.withColumn('tripTime',floor((unix_timestamp(df1.dropoff_datetime,format=timeFmt)-unix_timestamp(df1.pickup_datetime,format=timeFmt))/600))
	df1 = df1.drop('dropoff_datetime')

# Pickup datetime as date / time
if yearIdx in [2017,2015,2016]:
	df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('tpep_pickup_datetime', 'MM/dd/yyyy'))))
else:
	df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('pickup_datetime', 'MM/dd/yyyy'))))

# Day only
df1 = df1.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
df1 = df1.withColumn('month', month("yymmdd"))
# Date into year only
df1 = df1.withColumn('year', year("yymmdd"))

# Time in 24 
# if yearIdx in [2010,2011, 2012]:

if yearIdx in [2017,2015,2016]:
	df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('tpep_pickup_datetime')), 'HH')).drop('tpep_pickup_datetime')
else:
	df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('pickup_datetime')), 'HH')).drop('pickup_datetime')

# Drop store and fwd flag (~50%)
df1 = df1.drop('store_and_fwd_flag')

# Change the tip in percentage and cateogorize it 
df1 = df1.withColumn('tipAmount',floor(df1.tip_amount*10/df1.fare_amount)).drop('tip_amount')
# Trip distance / prices categorizing
df1 = df1.withColumn('tripDistance',floor(df1.trip_distance)).drop('trip_distance')
df1 = df1.withColumn('fareAmount',floor(df1.fare_amount/10)).drop('fare_amount')
df1 = df1.withColumn('tollsAmount',floor(df1.tolls_amount)).drop('tolls_amount')
df1 = df1.withColumn('totalAmount',floor(df1.total_amount/10)).drop('total_amount')
if yearIdx in [2015,2016,2017]:
	df1 = df1.withColumnRenamed('improvement_surcharge','surcharge')

df1 = df1.withColumn('surCharge',floor(df1.surcharge*10)).drop('surcharge')

df1 = df1.withColumnRenamed('id','unique_key')

df1.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("taxi_cleaned"+str(yearIdx))

##################################
# cleaning : bike
##################################
yearIdx = 2013

dt1 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"01-citibike-tripdata.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"02-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"03-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"04-citibike-tripdata.csv"))
dt2 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"05-citibike-tripdata.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"06-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"07-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"08-citibike-tripdata.csv"))
dt3 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"09-citibike-tripdata.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"10-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"11-citibike-tripdata.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/bike_zip/"+str(yearIdx)+"12-citibike-tripdata.csv"))
dt = dt1.union(dt2).union(dt3)

df1 = clean.fixColumnName(dt)
df1 = clean.zipcodeFilter(df1, 'start_zip')
df1 = df1.withColumn('startZipcode',df1.zipcode).drop('zipcode')
df1 = clean.zipcodeFilter(df1, 'end_zip')
df1 = df1.withColumn('endZipcode',df1.zipcode).drop('zipcode')

# Trip duration (By minitues and categorized by every 10 min)
if yearIdx in [2018, 2015,2014,2016,2017,2013]:
	df1 = df1.withColumn('tripTime',floor(df1.tripduration/(60*10))).drop('tripduration').drop('stopTime')
if yearIdx in [2017]:
	df1 = df1.withColumn('tripTime',floor(df1.trip_duration/(60*10))).drop('trip_duration').drop('stop_time')

# Pickup datetime as date / time
if yearIdx in [2018, 2015, 2016,2017,2013]:
	df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('starttime', 'MM/dd/yyyy'))))
if yearIdx in [2017]:
	df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('start_time', 'MM/dd/yyyy'))))
if yearIdx in [2014]:
	df1 = df1.withColumn('yymmdd',to_date(from_unixtime(unix_timestamp('starttime'))))

# Day only
df1 = df1.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
df1 = df1.withColumn('month', month("yymmdd"))
# Date into year only
df1 = df1.withColumn('year', year("yymmdd"))
# Time in 24 
if yearIdx in [2018,2013]:
	df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('starttime')), 'HH')).drop('starttime')
if yearIdx in [2017]:
	df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('start_time')), 'HH')).drop('start_time')
if yearIdx in [2015,2014,2016]:
	df1 = df1.withColumn('time24', split(split(col("starttime"),' ')[1],':')[0].cast("int")).drop('starttime')

# Age using birth year (Child:0,Teen:1, 20s:2, 30s:3..)
df1 = df1.withColumn('age',floor((yearIdx-df1.birth_year)/10)).drop('birth_year')

# station related info attributes are dropped since we have zip
df1 = df1.drop('bikeid','start_station_name','end_station_name','start_station_id','end_station_id')
if yearIdx in [2017]:
	df1 = df1.drop('bike_id')

# Change name of id -> unique_key
df1 = df1.withColumnRenamed('id','unique_key')

df1.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("bike_cleaned"+str(yearIdx))

##################################
# cleaning : crime
##################################
dt = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/crim_zip/NYPD_Complaint_Data_Historic.csv")

df1 = clean.fixColumnName(dt)
df1 = clean.zipcodeFilter(df1, 'zip')

# We have zip code -> delete coordinate id
df1 = df1.drop('x_coord_cd','y_coord_cd')

# Rename id -> unique_key
df1 = df1.withColumnRenamed('id','unique_key')

# Remove randomly generated complaint number
df1 = df1.drop('cmplnt_num')

# Remove descriptions since we have codes
df1 = df1.drop('ofns_desc','pd_desc')

# Delete attribute with "collecting if cases"
df1 = df1.drop('parks_nm','hadevelopt')

# Crime duration does not make sense -> delete
df1 = df1.drop('cmplnt_to_dt','cmplnt_to_tm','rpt_dt')

# Date complain started
df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('cmplnt_fr_dt', 'MM/dd/yyyy')))).drop('cmplnt_fr_dt')

# Day only
df1 = df1.withColumn('day', dayofmonth("yymmdd"))
# Date into month only
df1 = df1.withColumn('month', month("yymmdd"))
# Date into year only
df1 = df1.withColumn('year', year("yymmdd"))

# Time 
df1 = df1.withColumn('time24', split(col("cmplnt_fr_tm"),':')[0].cast("int")).drop("cmplnt_fr_tm")

df1.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("crime_cleaned")

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
