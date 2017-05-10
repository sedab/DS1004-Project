# Uber Data Processing

Data is collected from here: https://github.com/fivethirtyeight/uber-tlc-foil-response/tree/master/uber-trip-data


`uber_data_2014`: Pull daily summary of total uber rides in NYC for 2014, April - September.
To run this code, please download the 2014 data from github, add to HDFS on dumbo, and change the location in sc.textFile to where your data is located. Saves data to `uber_2014_daily.out`.

`uber_data_2015`: Pull daily summary of total uber rides in NYC for January - June.
To run this code, please download the 2015 zip data from github, unzip the file and add it to HDFS on dumbo. Change the location in sc.textFile to where your data is located. Saves data to `uber_2015_daily.out`.
