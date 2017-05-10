# Weather Data Analysis

* Data files are at HDFS:
Yellow cab: /user/ls1908/project/data/y
Green cab: /user/ls1908/project/data/g
Weather Data: /user/ls1908/project/hourly_weather_data_2013_through_2016.csv

* Location ID lookup file at HDFS ("taxi_zone_lookup.csv"):
/user/ls1908/project

* Python script for queries ("queries-spark2_latest.py") can be called by:
 spark2-submit queries-spark2_latest.py /user/ls1908/project/data/y/*.csv /user/ls1908/project/data/g/*.csv /user/ls1908/project/hourly_weather_data_2013_through_2016.csv

* Query Descriptions:
NOTE: When query has time of day, it refers to the hour. E.g.: if time of day = 10, this means 10:00 to 10:59.
1. Query1 - Counts of Payment Types per vendor
2. Query2 - Percentage of the different Rate Codes by hour of pick up time
3. Query3 - Percentage of store and forward flags by hour of pick up time
4. Query4 - Average total amount per payment type
5. Query5 - Average passenger count per time of day
6. Query6 - Number of Daily Trips 
7. Query7 - Percentage of trip distances per year
8. Query8 - Percentage of trip types per rate code (trip type applies to green cabs only)

* Hypothesis:
1. Hypothesis1 - Drop in yellow taxi rides on 2/22/2014 and 1/23/2016 is due to bad weather.

2. Increase in tip and total amounts due to bad weather.


**NOTE: All queries and hypotheses are commented out, it's recommended that each is uncommented and run at a time to save running time.
