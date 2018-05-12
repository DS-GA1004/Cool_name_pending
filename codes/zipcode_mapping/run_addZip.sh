

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'




#/usr/bin/hadoop fs -rm -r "$2_citibike_startGPS.txt" #"task$1.out"

if [ "$1" -eq 1 ]
then
#2013-07 - Citi Bike trip data
/usr/bin/hadoop fs -rm -r "/user/jn1664/nyc_data/bike_zip/$2-citibike-tripdata.csv"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python addZip.py citi /user/jn1664/nyc_data/bike/$2-citibike-tripdata.csv /user/jn1664/nyc_data/bike_zip/$2-citibike-tripdata.csv
#/usr/bin/hadoop fs -getmerge "/user/jn1664/nyc_data/bike_zip/$2-citibike-tripdata.csv" "$2/$2_citibike.csv" #"task$1.out" "$TMPFILE"


elif [ "$1" -eq 2 ]
then

/usr/bin/hadoop fs -rm -r "/user/jn1664/nyc_data/taxi_zip/yello_tripdata_$2.csv"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python addZip.py taxi /user/jn1664/nyc_data/taxi/yellow_tripdata_$2.csv /user/jn1664/nyc_data/taxi_zip/yello_tripdata_$2.csv
#/usr/bin/hadoop fs -getmerge "$2_taxi_startGPS.txt" "$2/$2_taxi_startGPS.txt" #"task$1.out" "$TMPFILE"


elif [ "$1" -eq 3 ]
then

/usr/bin/hadoop fs -rm -r "/user/jn1664/nyc_data/crim_zip/NYPD_Complaint_Data_Historic.csv"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python addZip.py crime /user/jn1664/nyc_data/NYPD_Complaint_Data_Historic.csv /user/jn1664/nyc_data/crim_zip/NYPD_Complaint_Data_Historic.csv
#/usr/bin/hadoop fs -getmerge "crime_GPS.txt" "crime/crime_GPS.txt" #"task$1.out" "$TMPFILE"


fi



