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
import cleaning_io as clean

##################################
# generate : weather
##################################
dt = spark.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/weather_cleaned")

df1 = dt.filter(dt.year == 2016).groupBy('month').avg('spd_floor')
df2 = dt.filter(dt.year == 2016).groupBy('month').avg('visb')
df3 = dt.filter(dt.year == 2016).groupBy('month').avg('temp_floor')
df4 = dt.filter(dt.year == 2016).groupBy('month').avg('prcp_floor')

df1 = df1.withColumnRenamed('month', 'monthA')
df2 = df2.withColumnRenamed('month', 'monthB')
df3 = df3.withColumnRenamed('month', 'monthC')
df4 = df4.withColumnRenamed('month', 'monthD')

a = df1.alias('a')
b = df2.alias('b')
c = df3.alias('c')
d = df4.alias('d')

df5 = a.join(b, a.monthA == b.monthB, 'inner').join(c, a.monthA == c.monthC).join(d, a.monthA == d.monthD).drop('monthB', 'monthC', 'monthD')
df5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("weather_plot_2016")

##################################
# generate : taxi
##################################
dt = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jy50/taxi_cleaned2016")

df1 = dt.filter(dt.year == 2016).groupBy('month').avg('tripTime')
df2 = dt.filter(dt.year == 2016).groupBy('month').avg('tipAmount')
df3 = dt.filter(dt.year == 2016).groupBy('month').avg('tripDistance')
df4 = dt.filter(dt.year == 2016).groupBy('month').avg('fareAmount')
df5 = dt.filter(dt.year == 2016).groupBy('month').avg('tollsAmount')
df6 = dt.filter(dt.year == 2016).groupBy('month').avg('totalAmount')

df1 = df1.withColumnRenamed('month', 'monthA')
df2 = df2.withColumnRenamed('month', 'monthB')
df3 = df3.withColumnRenamed('month', 'monthC')
df4 = df4.withColumnRenamed('month', 'monthD')
df5 = df5.withColumnRenamed('month', 'monthE')
df6 = df6.withColumnRenamed('month', 'monthF')

a = df1.alias('a')
b = df2.alias('b')
c = df3.alias('c')
d = df4.alias('d')
e = df5.alias('e')
f = df6.alias('f')

df7 = a.join(b, a.monthA == b.monthB, 'inner').join(c, a.monthA == c.monthC).join(d, a.monthA == d.monthD).join(e, a.monthA == e.monthE).join(f, a.monthA == f.monthF).drop('monthB', 'monthC', 'monthD', 'monthE', 'monthF')
df7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("taxi_plot_2016")


##################################
# generate : complain
##################################
dt = spark.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/complain_cleaned")

df1 = dt.filter(dt.year == 2016).groupBy('month').avg('spd_floor')
df2 = dt.filter(dt.year == 2016).groupBy('month').avg('visb')
df3 = dt.filter(dt.year == 2016).groupBy('month').avg('temp_floor')
df4 = dt.filter(dt.year == 2016).groupBy('month').avg('prcp_floor')

df1 = df1.withColumnRenamed('month', 'monthA')
df2 = df2.withColumnRenamed('month', 'monthB')
df3 = df3.withColumnRenamed('month', 'monthC')
df4 = df4.withColumnRenamed('month', 'monthD')

a = df1.alias('a')
b = df2.alias('b')
c = df3.alias('c')
d = df4.alias('d')

df5 = a.join(b, a.monthA == b.monthB, 'inner').join(c, a.monthA == c.monthC).join(d, a.monthA == d.monthD).drop('monthB', 'monthC', 'monthD')
df5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("weather_plot_2016")


