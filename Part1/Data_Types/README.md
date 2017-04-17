These scripts evaluate the data in each column of Yellow Cab Data and Green Cab data from August 2013 through December 2016. They are written using spark 2.0 and are compatible with python 2.0 and python 3.0. 


Columns that overlap between the datasets will use the same function. Columns that have the same content but a different naming convention ("VendorID" vs. "vendor\_id") have been grouped into the same column and will be evaluated using the same function.

Scripts are currently configured to save outputs to text files on hdfs: `column_y.out` for yellow output data, and `column_g.out` for green taxi output data. To view a snapshot rather than putting the total output to a file, comment out the lines in the scripts that include the function `.saveAsTextFile`.


## 1. Joint Yellow and Green Columns

Evaluate these columns by calling:

Using Spark 2:
`spark-submit column.py yellow_data_location green_data_location`

An example run on dumbo hdfs (spark2-submit to access spark 2):
`spark2-submit DOLocationID.py /user/ls1908/project/data/y/* /user/ls1908/project/data/g/*`

To test an individual datapoint:
`spark-submit column.py datapoint_input`

**Applies to columns:**
DOLocationID
dropoff_latitude
dropoff_longitude
extra
fare_amount
improvement_surcharge
mta_tax
passenger_count
payment_type
pickup_latitude
pickup_longitude
PULocationID
RateCodeID
store_and_fwd_flag
tip_amount
tolls_amount
total_amount
trip_distance
VendorID


## 2. Yellow Cab Data Specific Columns

`spark-submit column.py yellow_data_location 1`

To test an individual datapoint:
`spark-submit column.py datapoint_input`

**Applies to columns:**
tpep_dropoff_datetime
tpep_pickup_datetime


## 3. Green Cab Data Specific Columns

`spark-submit column.py green_data_location 1`

To test an individual datapoint:
`spark-submit column.py datapoint_input`

**Applies to columns:**
Lpep_dropoff_datetime
lpep_pickup_datetime
Ehail_fee
Trip_type
