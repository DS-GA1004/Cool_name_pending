

def process(yearIdx):
	dt1 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-01.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-02.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-03.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-04.csv"))
	dt2 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-05.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-06.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-07.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-08.csv"))
	dt3 = spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-09.csv").union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-10.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-11.csv")).union(spark.read.format('csv').options(header='true',inferschema='true').load("/user/jn1664/nyc_data/taxi_zip/yello_tripdata_"+str(yearIdx)+"-12.csv"))
	dt = dt1.union(dt2).union(dt3)
	df1 = clean.fixColumnName(dt)
	#df1 = clean.deleteNullAttr(df1)
	df1 = clean.zipcodeFilter(df1, 'start_zip')
	df1 = df1.withColumn('startZipcode',df1.zipcode).drop('zipcode')
	df1 = clean.zipcodeFilter(df1, 'end_zip')
	df1 = df1.withColumn('endZipcode',df1.zipcode).drop('zipcode')
	# Trip duration (By minitues and categorized by every 10 min)
	if yearIdx in [2010]:
		timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
		df1 = df1.withColumn('tripTime',floor((unix_timestamp(df1.dropoff_datetime,format=timeFmt)-unix_timestamp(df1.pickup_datetime,format=timeFmt))/600))
		df1 = df1.drop('dropoff_datetime')
	if yearIdx in [2017]:
		timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
		df1 = df1.withColumn('tripTime',floor((unix_timestamp(df1.tpep_dropoff_datetime,format=timeFmt)-unix_timestamp(df1.tpep_pickup_datetime,format=timeFmt))/600))
		df1 = df1.drop('tpep_dropoff_datetime')
	# Pickup datetime as date / time
	if yearIdx in [2010]:
		df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('pickup_datetime', 'MM/dd/yyyy'))))
	if yearIdx in [2017]:
		df1 = df1.withColumn('yymmdd', to_date(from_unixtime(unix_timestamp('tpep_pickup_datetime', 'MM/dd/yyyy'))))
	# Day only
	df1 = df1.withColumn('day', dayofmonth("yymmdd"))
	# Date into month only
	df1 = df1.withColumn('month', month("yymmdd"))
	# Date into year only
	df1 = df1.withColumn('year', year("yymmdd"))
	# Time in 24 
	if yearIdx in [2010]:
		df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('pickup_datetime')), 'HH')).drop('pickup_datetime')
	if yearIdx in [2017]:
		df1 = df1.withColumn('time24', date_format(from_unixtime(unix_timestamp('tpep_pickup_datetime')), 'HH')).drop('tpep_pickup_datetime')
	# Drop store and fwd flag (~50%)
	df1 = df1.drop('store_and_fwd_flag')
	# Change the tip in percentage and cateogorize it 
	df1 = df1.withColumn('tipAmount',floor(df1.tip_amount*10/df1.fare_amount)).drop('tip_amount')
	# Trip distance / prices categorizing
	df1 = df1.withColumn('tripDistance',floor(df1.trip_distance)).drop('trip_distance')
	df1 = df1.withColumn('fareAmount',floor(df1.fare_amount/10)).drop('fare_amount')
	df1 = df1.withColumn('tollsAmount',floor(df1.tolls_amount)).drop('tolls_amount')
	df1 = df1.withColumn('totalAmount',floor(df1.total_amount/10)).drop('total_amount')
	df1 = df1.withColumn('surCharge',floor(df1.surcharge*10)).drop('surcharge')
	df1 = df1.withColumnRenamed('id','unique_key')
	df1.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").save("bike_cleaned"+str(yearIdx))

for yearIdx in range(2011, 2017):
	process(yearIdx)
