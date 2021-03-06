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


##################################
# Inner Join and filter by year
##################################
# find common name column and change it 
def process(df1, df2, year):
        colA = df1.columns
        colB = df2.columns
        colAB = list(set(colA).intersection(colB))

        for colab in colAB:
        	df1 = df1.withColumnRenamed(colab,colab+'A')

        a = df1.alias('a')
        b = df2.alias('b')

        dfJoin = a.join(b, a.yymmddA == b.yymmdd, 'inner').select([col('a.'+xx) for xx in a.columns] + [col('b.'+xx) for xx in b.columns])
        # Delete duplicated columns

        colDel = ['yymmdd','day','month','year','unique_key']
        for colab in colDel:
                dfJoin = dfJoin.drop(colab+'A')
        # for colab in colAB:
        # 	dfJoin = dfJoin.drop(colab+'A')

##################################
# Mutual information (Year by year)
##################################
# find common name column and change it 

# years = dfJoin.select('year').distinct().rdd.map(lambda x: x[0]).collect()
        dfJoin = dfJoin.filter(col('year') == year)

        uniqueAttr = 'unique_key'
        attributes = dfJoin.columns
        attributes.remove(uniqueAttr)
        MImat = np.zeros((len(attributes), len(attributes)))

        for i in range(0,len(attributes)):
                for j in range(i+1,len(attributes)):
                        print("row,col:",i,j)
                        attr1 = attributes[i]
                        attr2 = attributes[j]
                        MImat[i][j] = mi.mutual_information(dfJoin, attr1, attr2, uniqueAttr,0,'')

        rdd1 = sc.parallelize(MImat)
        rdd2 = rdd1.map(lambda x: [float(i) for i in x])
        MImatdf = rdd2.toDF(attributes)
        MImatdf.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("property_complain"+str(year))


df1 = sqlCtx.read.format('csv').options(header='true',inferschema='true').load("/user/hk2451/complain_cleaned")
years = [int(sys.argv[1])]
print("================================================================================")
print(years)
property_file = "property"+str(years[0]-2000)
df2 = sqlCtx.read.format('csv').options(header='true',inferschema='true').load(property_file)
process(df1, df2, years[0])

sc.stop()

