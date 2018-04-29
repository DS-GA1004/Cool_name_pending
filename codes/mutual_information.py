#!/usr/bin/env python

# - import common modules
import sys
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col
import datetime

def mutual_information(dt, attr1, attr2, uniqueAttr,saveFlag,saveName):	
	"""Mutual information calculation function

	Args:
	dt (DataFrame): DataFrame contains attributes
	attr1 (str): Attribute name
	attr2 (str): Attribute name
	saveFlag (bool): If you want to save for details, select "True"
	saveName (str): DataFrame contains attribute pair, pX, pY, pXY, and MI values will be saved under "saveName"

	Returns:
	Mutual information value

	"""

	totalRow = dt.count()
	nC1 = [attr1, "count"]
	nC2 = [attr2, "count"]
	nC3 = [attr1,attr2,"count"]
	# Count for attribute 1
	xx = dt.select(attr1).groupBy(attr1).agg(countDistinct(uniqueAttr)).toDF(*nC1).withColumn("pX", (col("count"))/(totalRow))
	# Count for attribute 2
	yy = dt.select(attr2).groupBy(attr2).agg(countDistinct(uniqueAttr)).toDF(*nC2).withColumn("pY", (col("count"))/(totalRow))
	# Count for co-existing rows for attrbute 1 and attribute 2
	xy = dt.select(attr1,attr2).groupBy(attr1,attr2).agg(countDistinct(uniqueAttr)).toDF(*nC3).withColumn("pXY", (col("count"))/(totalRow))

	# Join to get attribute pairs' matching pXY, pX, pY
	a = xy.alias('a')
	b = xx.alias('b')
	c = yy.alias('c')
	xy_x = a.join(b, col('a.'+attr1) == col('b.'+attr1), 'inner').select(col('a.'+attr1),col('a.'+attr2),col('a.'+'pXY'),col('b.'+'pX'))
	d = xy_x.alias('d')
	xy_xy = d.join(c, col('d.'+attr2) == col('c.'+attr2), 'inner').select(col('d.'+attr1),col('d.'+attr2),col('d.'+'pXY'),col('d.'+'pX'),col('c.'+'pY'))

	# Calculate mutual information
	xy_xy = xy_xy.withColumn("MI", (col("pXY") * log(col("pXY") / (col("pX") * col("pY")))))

	if saveFlag == True:
		# Save dataframe with pX, pY, pXY for later use
		xy_xy.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save(saveName)

	# Return mutual information result between attribute 1 and attribute 2
	return xy_xy.select(col("MI")).rdd.map(lambda x: x[0]).sum()

	
