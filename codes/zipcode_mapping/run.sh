

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'



#SPARKCODE=$(echo "$2/task$1".py)
#TMPFILE="$2/task$1tmp.out"

#/usr/bin/hadoop fs -rm -r "$2_citibike_startGPS.txt" #"task$1.out"

if [ "$1" -eq 1 ]
then
#2013-07 - Citi Bike trip data
/usr/bin/hadoop fs -rm -r "$2_citibike_startGPS.txt"

#spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py citi_start /user/jn1664/nyc_data/bike/$2 - Citi Bike trip data.csv
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py citi_start /user/jn1664/nyc_data/bike/$2-citibike-tripdata.csv
/usr/bin/hadoop fs -getmerge "$2_citibike_startGPS.txt" "$2/$2_citibike_startGPS.txt" #"task$1.out" "$TMPFILE"


/usr/bin/hadoop fs -rm -r "$2_citibike_endGPS.txt"

#spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py citi_end /user/jn1664/nyc_data/bike/$2 - Citi Bike trip data.csv
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py citi_end /user/jn1664/nyc_data/bike/$2-citibike-tripdata.csv
/usr/bin/hadoop fs -getmerge "$2_citibike_endGPS.txt" "$2/$2_citibike_endGPS.txt" #"task$1.out" "$TMPFILE"


elif [ "$1" -eq 2 ]
then

/usr/bin/hadoop fs -rm -r "$2_taxi_startGPS.txt"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py taxi_start /user/jn1664/nyc_data/taxi/yellow_tripdata_$2.csv
/usr/bin/hadoop fs -getmerge "$2_taxi_startGPS.txt" "$2/$2_taxi_startGPS.txt" #"task$1.out" "$TMPFILE"

/usr/bin/hadoop fs -rm -r "$2_taxi_endGPS.txt"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py taxi_end /user/jn1664/nyc_data/taxi/yellow_tripdata_$2.csv
/usr/bin/hadoop fs -getmerge "$2_taxi_endGPS.txt" "$2/$2_taxi_endGPS.txt" #"task$1.out" "$TMPFILE"


elif [ "$1" -eq 3 ]
then

/usr/bin/hadoop fs -rm -r "crime_GPS.txt"

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python cutGPS.py crime /user/jn1664/nyc_data/NYPD_Complaint_Data_Historic.csv
/usr/bin/hadoop fs -getmerge "crime_GPS.txt" "crime/crime_GPS.txt" #"task$1.out" "$TMPFILE"


fi



