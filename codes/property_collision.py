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

years = [int(sys.argv[1])]
print("================================================================================")
print(years)

property_file = "property"+str(years[0]-2000)
df1 = sqlCtx.read.format('csv').options(header='true',inferschema='true').load(property_file)
df2 = sqlCtx.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/collision_cleaned")


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

colDel = ['yymmdd','day','month','year']
for colab in colDel:
        dfJoin = dfJoin.drop(colab+'A')

##################################
# Mutual information (Year by year)
##################################
# find common name column and change it 

k=0
dfJoin = dfJoin.filter(col('year') == years[k])

uniqueAttr = 'unique_key'
attributes = dfJoin.columns
attributes.remove(uniqueAttr)
MImat = np.zeros((len(attributes), len(attributes)))

for i in range(0,len(attributes)):
        for j in range(i+1,len(attributes)):
                print("row,col:",i,j)
                attr1 = attributes[i]
                attr2 = attributes[j]
                mimi = mi.mutual_information(dfJoin, attr1, attr2, uniqueAttr,0,'')
                print("mutual: %f" % (mimi))
                MImat[i][j] = mimi

rdd1 = sc.parallelize(MImat)
rdd2 = rdd1.map(lambda x: [float(i) for i in x])
MImatdf = rdd2.toDF(attributes)
MImatdf.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("property_collision"+str(years[k]))

sc.stop()