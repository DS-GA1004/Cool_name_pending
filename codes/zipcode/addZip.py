from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from pyspark.sql.functions import format_string
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


if __name__=="__main__":
    #if len(sys.argv) != 2:
	#print("Usage wrong")
	#exit(-1)
    sc = SparkContext()
    source = sys.argv[1]


    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#+++++++++++++++++++++++++++++++++++++++++++++++++citi+++++++++++++++++++++++++++++++++
    if source == 'citi':
        date = sys.argv[2].split("/")[5].split("-")[0]
        zip_dir = 'zipcode/bike/'

        #1. data read
        ori_data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
        startZip = spark.read.format('csv').options(header='true',inferschema='true').load(zip_dir+date+'_citibike_startZip.txt')
        endZip = spark.read.format('csv').options(header='true',inferschema='true').load(zip_dir+date+'_citibike_endZip.txt')


        #2. make index
        #ori_data = ori_data.select("*").withColumn("id", F.monotonically_increasing_id())
        ori_data = ori_data.rdd.zipWithIndex().toDF()
        startZip = startZip.rdd.zipWithIndex().toDF()
        endZip = endZip.rdd.zipWithIndex().toDF()

        #3. change column names
        #"tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","name_localizedValue0","usertype","birth year","gender"
        #Trip Duration,Start Time,Stop Time,Start Station ID,Start Station Name,Start Station Latitude,Start Station Longitude,End Station ID,End Station Name,End Station Latitude,End Station Longitude,Bike ID,User Type,Birth Year,Gender
        if ('201610' in sys.argv[2]) or ('201701' in sys.argv[2]) or ('201702' in sys.argv[2]) or ('201703' in sys.argv[2]):
            ori_data = ori_data.select(F.col("_1.Trip Duration").alias("Trip Duration"), F.col("_1.Start Time").alias("Start Time"), F.col("_1.Stop Time").alias("Stop Time"),F.col("_1.Start Station ID").alias("Start Station ID"), F.col("_1.Start Station Name").alias("Start Station Name"), F.col("_1.End Station ID").alias("End Station ID"), F.col("_1.End Station Name").alias("End Station Name"), F.col("_1.Bike ID").alias("Bike ID"), F.col("_1.User Type").alias("User Type"), F.col("_1.Birth Year").alias("Birth Year"), F.col("_1.Gender").alias("Gender"), F.col("_2").alias("id"))

        else:
            ori_data = ori_data.select(F.col("_1.tripduration").alias("tripduration"), F.col("_1.starttime").alias("starttime"), F.col("_1.stoptime").alias("stoptime"),F.col("_1.start station id").alias("start station id"), F.col("_1.start station name").alias("start station name"), F.col("_1.end station id").alias("end station id"), F.col("_1.end station name").alias("end station name"), F.col("_1.bikeid").alias("bikeid"), F.col("_1.usertype").alias("usertype"), F.col("_1.birth year").alias("birth year"), F.col("_1.gender").alias("gender"), F.col("_2").alias("id"))
        startZip = startZip.select(F.col("_1.Zipcode").alias("start_zip"), F.col("_2").alias("id"))
        endZip = endZip.select(F.col("_1.Zipcode").alias("end_zip"), F.col("_2").alias("id"))

        #4. add zip to ori_data
        #ori_data = ori_data.join(startZip, 'id').join(endZip, 'id')
        ori_data = ori_data.join(startZip, 'id').join(endZip, 'id')
        ori_data.write.csv(sys.argv[3], header=True)
        #ori_data.map(lambda x: '%s %s' %(x[1], x[0])).saveAsTextFile(sys.argv[3])
#++++++++++++++++++++++++++++++++++++++++++++++++taxi++++++++++++++++++++++++++++++++++++
    if source == 'taxi':
        date = sys.argv[2].split("/")[5].split(".")[0].split("_")[2].replace("-","")
        zip_dir = "zipcode/taxi/"

        #1. data read
        ori_data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
        startZip = spark.read.format('csv').options(header='true',inferschema='true').load(zip_dir + date + '_taxi_startZip.txt')
        endZip = spark.read.format('csv').options(header='true',inferschema='true').load(zip_dir+date+'_taxi_endZip.txt')

        #2. make index
        ori_data = ori_data.rdd.zipWithIndex().toDF(sampleRatio=0.2)
        startZip = startZip.rdd.zipWithIndex().toDF()
        endZip = endZip.rdd.zipWithIndex().toDF()


        #3. change column names
        #2009-01: vendor_name, Trip_Pickup_DateTime, Trip_Dropoff_DateTime, Passenger_Count, Trip_Distance, Start_Lon, Start_Lat, Rate_Code, store_and_forward, End_Lon, End_Lat, Payment_Type, Fare_Amt, surcharge, mta_tax, Tip_Amt, Tolls_Amt, Total_Amt
        #2010-01~:vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
        #2015-01~: VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RatecodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
        print(sys.argv[2])
        print(date)

        if '2009' in date:
            ori_data = ori_data.select(F.col("_1.vendor_name").alias("vendor_name"),F.col("_1.Trip_Pickup_DateTime").alias("Trip_Pickup_DateTime"),F.col("_1.Trip_Dropoff_DateTime").alias("Trip_Dropoff_DateTime"),F.col("_1.Passenger_Count").alias("Passenger_Count"),F.col("_1.Trip_Distance").alias("Trip_Distance"),F.col("_1.Rate_Code").alias("Rate_Code"),F.col("_1.store_and_forward").alias("store_and_forward"),F.col("_1.Payment_Type").alias("Payment_Type"),F.col("_1.Fare_Amt").alias("Fare_Amt"),F.col("_1.surcharge").alias("surcharge"),F.col("_1.mta_tax").alias("mta_tax"),F.col("_1.Tip_Amt").alias("Tip_Amt"),F.col("_1.Tolls_Amt").alias("Tolls_Amt"),F.col("_1.Total_Amt").alias("Total_Amt"), F.col("_2").alias("id"))

            ori_data = ori_data.withColumn("Rate_Code", F.when(ori_data.Rate_Code=="", F.lit("null")))
            ori_data = ori_data.withColumn("store_and_forward", F.when(ori_data.store_and_forward=="", F.lit("null")))
            ori_data = ori_data.withColumn("mta_tax", F.when(ori_data.mta_tax=="", F.lit("null")))

        elif ('2015' in date) or ('2016' in date):
            ori_data = ori_data.select(F.col("_1.VendorID").alias("VendorID"),F.col("_1.tpep_pickup_datetime").alias("tpep_pickup_datetime"),F.col("_1.tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),F.col("_1.passenger_count").alias("passenger_count"),F.col("_1.trip_distance").alias("trip_distance"),F.col("_1.RatecodeID").alias("RatecodeID"),F.col("_1.store_and_fwd_flag").alias("store_and_fwd_flag"),F.col("_1.payment_type").alias("payment_type"),F.col("_1.fare_amount").alias("fare_amount"),F.col("_1.extra").alias("extra"),F.col("_1.mta_tax").alias("mta_tax"),F.col("_1.tip_amount").alias("tip_amount"),F.col("_1.tolls_amount").alias("tolls_amount"),F.col("_1.improvement_surcharge").alias("improvement_surcharge"),F.col("_1.total_amount").alias("total_amount"), F.col("_2").alias("id"))

        elif ('2010' in date) or ('2011' in date) or ('2012' in date) or ('2013' in date):
            ori_data = ori_data.select(F.col("_1.vendor_id").alias("vendor_id"),F.col("_1.pickup_datetime").alias("pickup_datetime"),F.col("_1.dropoff_datetime").alias("dropoff_datetime"),F.col("_1.passenger_count").alias("passenger_count"),F.col("_1.trip_distance").alias("trip_distance"),F.col("_1.rate_code").alias("rate_code"),F.col("_1.store_and_fwd_flag").alias("store_and_fwd_flag"),F.col("_1.payment_type").alias("payment_type"),F.col("_1.fare_amount").alias("fare_amount"),F.col("_1.surcharge").alias("surcharge"),F.col("_1.mta_tax").alias("mta_tax"),F.col("_1.tip_amount").alias("tip_amount"),F.col("_1.tolls_amount").alias("tolls_amount"),F.col("_1.total_amount").alias("total_amount"), F.col("_2").alias("id"))

        else:
            print("working on 2014...")
            ori_data = ori_data.select(F.col("_1.vendor_id").alias("vendor_id"),F.col("_1. pickup_datetime").alias("pickup_datetime"),F.col("_1. dropoff_datetime").alias("dropoff_datetime"),F.col("_1. passenger_count").alias("passenger_count"),F.col("_1. trip_distance").alias("trip_distance"),F.col("_1. rate_code").alias("rate_code"),F.col("_1. store_and_fwd_flag").alias("store_and_fwd_flag"),F.col("_1. payment_type").alias("payment_type"),F.col("_1. fare_amount").alias("fare_amount"),F.col("_1. surcharge").alias("surcharge"),F.col("_1. mta_tax").alias("mta_tax"),F.col("_1. tip_amount").alias("tip_amount"),F.col("_1. tolls_amount").alias("tolls_amount"),F.col("_1. total_amount").alias("total_amount"), F.col("_2").alias("id"))

        startZip = startZip.select(F.col("_1.Zipcode").alias("start_zip"), F.col("_2").alias("id"))
        endZip = endZip.select(F.col("_1.Zipcode").alias("end_zip"), F.col("_2").alias("id"))


        if '2016' not in sys.argv[2]:
            ori_data = ori_data.withColumn("id", ori_data.id + 1)

        #3. add zip to ori data 
        ori_data = ori_data.join(startZip, 'id').join(endZip, 'id')
        ori_data.write.csv(sys.argv[3], header=True)
        #ori_data.saveAsTextFile(sys.argv[3])
#++++++++++++++++++++++++++++++++++++++++++++crime+++++++++++++++++++++++++++++++++===
    if source == 'crime':
        zip_dir = 'zipcode/crime/'

        #1. data read
        ori_data = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
        zip = spark.read.format('csv').options(header='true',inferschema='true').load(zip_dir+'crime_zip.txt')

        #2. make index
        ori_data = ori_data.rdd.zipWithIndex().toDF()
        zip = zip.rdd.zipWithIndex().toDF()

        #3. change column names
        #CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon
        ori_data = ori_data.select(F.col("_1.CMPLNT_NUM").alias("CMPLNT_NUM"),F.col("_1.CMPLNT_FR_DT").alias("CMPLNT_FR_DT"),F.col("_1.CMPLNT_FR_TM").alias("CMPLNT_FR_TM"),F.col("_1.CMPLNT_TO_DT").alias("CMPLNT_TO_DT"),F.col("_1.CMPLNT_TO_TM").alias("CMPLNT_TO_TM"),F.col("_1.RPT_DT").alias("RPT_DT"),F.col("_1.KY_CD").alias("KY_CD"),F.col("_1.OFNS_DESC").alias("OFNS_DESC"),F.col("_1.PD_CD").alias("PD_CD"),F.col("_1.PD_DESC").alias("PD_DESC"),F.col("_1.CRM_ATPT_CPTD_CD").alias("CRM_ATPT_CPTD_CD"),F.col("_1.LAW_CAT_CD").alias("LAW_CAT_CD"),F.col("_1.JURIS_DESC").alias("JURIS_DESC"),F.col("_1.BORO_NM").alias("BORO_NM"),F.col("_1.ADDR_PCT_CD").alias("ADDR_PCT_CD"),F.col("_1.LOC_OF_OCCUR_DESC").alias("LOC_OF_OCCUR_DESC"),F.col("_1.PREM_TYP_DESC").alias("PREM_TYP_DESC"),F.col("_1.PARKS_NM").alias("PARKS_NM"),F.col("_1.HADEVELOPT").alias("HADEVELOPT"),F.col("_1.X_COORD_CD").alias("X_COORD_CD"),F.col("_1.Y_COORD_CD").alias("Y_COORD_CD"), F.col("_2").alias("id"))
        
        zip = zip.select(F.col("_1.Zipcode").alias("zip"), F.col("_2").alias("id"))

        #add zip to ori data
        ori_data = ori_data.join(zip, 'id')
        ori_data.write.csv(sys.argv[3], header=True)
        #ori_data.saveAsTextFile(sys.argv[3])

    sc.stop()
