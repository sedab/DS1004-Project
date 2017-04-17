* Data files are at HDFS:
Yellow cab: /user/ls1908/project/data/y
Green cab: /user/ls1908/project/data/g

* Location ID lookup file at HDFS ("taxi_zone_lookup.csv"):
/user/ls1908/project

* Python script for queries ("queries-spark2.py") can be called by:
spark2-submit queries-spark2.py /user/ls1908/project/data/y/*.csv /user/ls1908/project/data/g/*.csv

* Query Descriptions:
NOTE: When query has time of day, it refers to the hour. E.g.: if time of day = 10, this means 10:00 to 10:59.
1. Query1 - Counts of Payment Types per vendor
2. Query2 - Percentage of the different Rate Codes by hour of pick up time
3. Query3 - Percentage of store and forward flags by hour of pick up time
4. Query4 - Average total amount per payment type
