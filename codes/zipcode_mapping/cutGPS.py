from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql.functions import format_string
from pyspark.sql import SparkSession



if __name__=="__main__":
    #if len(sys.argv) != 2:
	#print("Usage wrong")
	#exit(-1)
    sc = SparkContext()
    source = sys.argv[1]
	
    data = sc.textFile(sys.argv[2], 1)


#+++++++++++++++++++++++++++++++++++++++++++++++++citi+++++++++++++++++++++++++++++++++
    if source == 'citi_start':
        data = data.mapPartitions(lambda x: reader(x))

        outfile = sys.argv[2].split("/")[5]
        outfile = outfile.split("-")[0]


        #"tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","usertype","birth year","gender"
        startGPS = data.map(lambda x: (x[5], x[6]))
        startGPS.map(lambda x: '%s %s' %(x[1], x[0])).saveAsTextFile(outfile+'_citibike_startGPS.txt')

    if source == 'citi_end':
        data = data.mapPartitions(lambda x: reader(x))

        outfile = sys.argv[2].split("/")[5]
        outfile = outfile.split("-")[0]

        endGPS = data.map(lambda x: (x[9], x[10]))
        endGPS.map(lambda x: '%s %s' %(x[1], x[0])).saveAsTextFile(outfile+'_citibike_endGPS.txt')

#++++++++++++++++++++++++++++++++++++++++++++++++taxi++++++++++++++++++++++++++++++++++++
    if source == 'taxi_start':
        spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

        data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
        startGPS = data.rdd.map(lambda x: (x[5], x[6]))

        outfile = sys.argv[2].split("/")[5].split(".")[0]
        outfile = outfile.split("_")[2]

	#vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
        #startGPS = data.map(lambda x: (x[5], x[6]))
        startGPS.map(lambda x: '%s %s' %(x[0], x[1])).saveAsTextFile(outfile+'_taxi_startGPS.txt')

    if source == 'taxi_end':
        spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

        data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])#"/user/jn1664/nyc_data/yellow_tripdata_2012-01.csv")
        endGPS = data.rdd.map(lambda x: (x[9], x[10]))


        outfile = sys.argv[2].split("/")[5].split(".")[0]
        outfile = outfile.split("_")[2]

        #endGPS = data.map(lambda x: (x[9], x[10]))
        endGPS.map(lambda x: '%s %s' %(x[0], x[1])).saveAsTextFile(outfile+'_taxi_endGPS.txt')

#++++++++++++++++++++++++++++++++++++++++++++crime+++++++++++++++++++++++++++++++++===
    if source == 'crime':
        spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

        data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
        startGPS = data.rdd.map(lambda x: (x[21], x[22]))

	#CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon
        startGPS.map(lambda x: '%s %s' %(x[1], x[0])).saveAsTextFile('crime_GPS.txt')

    sc.stop()
