# Correlation analysis on spatiotemporal data

## Table of Contents
* [Project Description](#project-description)
* [Team Members](#team-members)
* [Installation and How to use](#codes)

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
 ### Clone
 - Clone this repo to your local machine using `https://github.com/DS-GA1004/Cool_name_pending.git`
 ### Data cleaning 
 You need to specify your directory to use functions and files in the script. For example, you may want functions defined in "cleaning_io.py" file in the script. "script_cleaning.py" script includes cleaning codes for all dataset. You can comment/uncomment the script. You can add files as follows:
 ```python
 from pyspark.sql import SparkSession
 from pyspark.sql import SQLContext
 from pyspark import SparkConf, SparkContext
 # If you running pyspark in local:
 sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
 import cleaning_io as clean
 # If you running pyspark in hdfs:
 conf = SparkConf().setAppName("building a warehouse")
 sc = SparkContext(conf=conf)
 sqlCtx = SQLContext(sc)
 sc.addFile("/home/hk2451/project/Cool_name_pending/codes/cleaning/cleaning_io.py")
 import cleaning_io as clean
 ```
 
 * Data cleaning
 * Correlation Calculation
 * Visualization
 
