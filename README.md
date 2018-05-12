# Correlation analysis on spatiotemporal data

## Table of Contents
- [Project Description](#project-description)
- [Team Members](#team-members)
- [Installation and How to use](#codes)
  * [Clone](#clone)
  * [Data cleaning and Aggregation](#data-cleaning)
  * [Mutual information](#MI)
  * [Visualization and Analysis](#visualization)
---

## <a name="project-description"><a/>Team name: Cool name pending (Big Data (DS-GA 1004) / 18' Spring)
 In terms of big data, we often face a lot of data issues. So we target the main data quality issues in the 7 datasets we process. And then we demonstrate the rules to do the data cleaning and aggregation which can be a guide for people who deal with big urban data. In this project, we calculate mutual information inside a dataset and cross different datasets. The correlation analysis based on the data processing and facts really help to make insights on the city functions and make better decisions on urban planning. 
  In this project, we introduce the pipeline of correlation analysis based on the spatiotemporal dataset. Our project follows the steps of data cleaning, Data integration, correlation calculation, visualization and analysis. 

![Picture](https://github.com/DS-GA1004/Cool_name_pending/blob/master/pipeline.png)

---

## <a name="team-members"><a/>Team Members
  * "Heejong Kim (hk2451)" <https://github.com/heejong-kim>
  * "Jun Yuan (jy50)" <https://github.com/junyuanjun>
  * "Jiin Nam (jn1664)" <https://github.com/Namssi>

---

## <a name="codes"><a/>Installation and How to use
 ### <a name="clone"><a/>Clone
 - Clone this repo to your local machine using `https://github.com/DS-GA1004/Cool_name_pending.git`
 ### <a name="data-cleaning"><a/>Data cleaning and aggregation
 1. Overall cleaning and aggregation. You need to specify your directory to use functions and files in the script. For example, you may want functions defined in "Cool_name_pending/codes/cleaning_io.py" file in the script. "Cool_name_pending/codes/script_cleaning.py" script includes cleaning codes for all dataset. You can comment/uncomment the script. You can add files as follows:
 ```python
 from pyspark.sql import SparkSession
 from pyspark.sql import SQLContext
 from pyspark import SparkConf, SparkContext
 
 # If you running pyspark in local:
 sc.addFile("where your files are located/cleaning_io.py")
 import cleaning_io as clean
 
 # If you running pyspark in hdfs:
 conf = SparkConf().setAppName("building a warehouse")
 sc = SparkContext(conf=conf)
 sqlCtx = SQLContext(sc)
 sc.addFile("where your files are located/cleaning_io.py")
 import cleaning_io as clean
 ```
 2. Convert longitude latitude coordinate to zipcode. There are dataset having longitude and latitude coordinate only. We converted the information separately using java. The script and related files are under "Cool_name_pending/codes/zipcode_mapping".
 Pipeline for zipcode mapping algorithm:
 
 <img align="center" src="https://github.com/DS-GA1004/Cool_name_pending/blob/master/ZipMapping.png" width="700">
 
 ```sh
 #1. how to run
	#./run.sh dataset_num date(in the shape for each dataset)
	#ex for citibike) ./run.sh 1 201803
	#ex for taxi)     ./run.sh 2 2012-01
 #     ./run_addZip_all.sh dataset_num date(in the shape for each dataset)
 #     ex for citibike) ./run_addZip_all.sh 1 201803
 #2. dataset_num
	# 1:citibike
	# 2:taxi
	# 3:crime
 
 # example for taxi dataset
 ./run_addZip_all.sh 2 2012-01
 ```

  ### <a name="MI"><a/>Mutual information
  As you did for the cleaning step, you need to specify your directory to use functions and files in the script. ([Adding python files with functions in the script](#data-cleaning))

 The python files under "Cool_name_pending/codes/MIexample" are example script for calculating mutual information. 

 To run consecutively, check out shell scripts:
  - taskByYear.sh
  - taskByYearAll.sh
  Example usage:
  ```sh
  # If you running pyspark in local:
  ./taskByYear.sh 2012 "collision_weather" ./Cool_name_pending/codes/MIexample/collision_weather_year
  # If you running pyspark with spark-submit
  ./taskByYear.sh 2012 "collision_weather" ./Cool_name_pending/codes/MIexample/script_spark_submit_collision_weather
  ```
  
 You can modify filenames for new cleaned datasets. 
 For local use, check these files:
  - collision_weather_year.py
  - complain_collision_year.py
  - complain_weather_year.py

 For hdfs use (for spark-sumit):
  - script_spark_submit_collision_weather.py
  - script_spark_submit_complain_weather.py
  - script_spark_submit_complaint_collision.py

  ### <a name="Visualization"><a/>Visualization and Analysis
  We used matlab and python for analyzing results and drawing plots. All files related to visualization and analysis are located under "Cool_name_pending/codes/visualization"
  For cleaning result, check the file:
        - cleaning_bar_chart.py
  Example output for bar chart:
	
  <img align="center" src="https://github.com/DS-GA1004/Cool_name_pending/blob/master/issue_columns.png" width="700">

	
  For correlation result, check the file:
        - script_analyze_and_draw.m
        The script has dependencies which are:
                	- [cbrewer][1]
                	- [readline][2]
		
  Example output for correlation matrix and line chart:
  
    <img align="center" src="https://github.com/DS-GA1004/Cool_name_pending/blob/master/weather_taxi2016.png" width="700">
    
    <img align="center" src="https://github.com/DS-GA1004/Cool_name_pending/blob/master/weather_taxi_temporal_g1.png" width="700">
    
    
    
  [1]:https://www.mathworks.com/matlabcentral/fileexchange/34087-cbrewer---colorbrewer-schemes-for-matlab
  [2]:https://www.mathworks.com/matlabcentral/fileexchange/20026-readline-m-v4-0


