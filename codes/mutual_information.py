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
import datetime



# Read data in data frame format
dt = spark.read.format('csv').options(header='true',inferschema='true').load("file:///home/hk2451/project/cleaned/cleaned311.csv")

attrIn1 = dt.select('Agency').distinct().rdd.flatMap(lambda x: x).collect()
attrIn2 = dt.select('Community_Board').distinct().rdd.flatMap(lambda x: x).collect()

totalRow = dt.count()

pXY = np.zeros((len(attrIn1), len(attrIn2)))
pX = np.zeros((len(attrIn1),1))
pY = np.zeros((len(attrIn2), 1))

for i in range(0,len(attrIn1)):
	for j in range(i,len(attrIn2)):
        pXY[i][j] = (dt.filter(dt["Agency"] == attrIn1[i]).filter(dt["Community_Board"] == attrIn2[j]).count())/totalRow

for i in range(0,len(attrIn1)):
    pX[i] = (dt.filter(dt["Agency"] == attrIn1[i]).count())/totalRow

for i in range(0,len(attrIn2)):
    pY[i] = (dt.filter(dt["Community_Board"] == attrIn2[i]).count())/totalRow

pXpY = pX.dot(pY.transpose())

mask = pXY != 0

MI = np.sum(np.multiply(pXY[mask], np.log(np.divide(pXY[mask], pXpY[mask]))))