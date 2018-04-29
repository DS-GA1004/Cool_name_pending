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
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)

sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
sc.addFile("/home/hk2451/project/Cool_name_pending/codes/mutual_information.py")

import cleaning_io as clean
import mutual_information as mi

dt = sqlCtx.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/join_year_2012_2")

uniqueAttr = 'unique_key'
attributes = dt.columns
attributes.remove(uniqueAttr)
MImat = np.zeros((len(attributes), len(attributes)))

for i in range(0,len(attributes)):
	for j in range(i+1,len(attributes)):
		attr1 = attributes[i]
		attr2 = attributes[j]
		MImat[i][j] = mi.mutual_information(dt, attr1, attr2, uniqueAttr,0,'')

rdd1 = sc.parallelize(MImat)
rdd2 = rdd1.map(lambda x: [float(i) for i in x])
MImatdf = rdd2.toDF(attributes)
MImatdf.write.format("com.databricks.spark.csv").option("delimiter",",").save("collision_complain_2012-1")

