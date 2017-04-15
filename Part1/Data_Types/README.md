These scripts evaluate the data in each column of Yellow Cab Data and Green Cab data from August 2013 through December 2016.


Columns that overlap between the datasets will use the same function. Columns that have the same content but a different naming convention ("VendorID" vs. "vendor\_id") have been grouped into the same column and will be evaluated using the same function.

## 1. Joint Yellow and Green Columns

Evaluate these columns by calling:

`column.py yellow_data_location green_data_location`

To test an individual datapoint:
`column.py datapoint_input`

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

`column.py yellow_data_location 1`

To test an individual datapoint:
`column.py datapoint_input`

**Applies to columns:**
tpep_dropoff_datetime
tpep_pickup_datetime


## 3. Green Cab Data Specific Columns

`column.py green_data_location 1`

To test an individual datapoint:
`column.py datapoint_input`

**Applies to columns:**
Lpep_dropoff_datetime
lpep_pickup_datetime
Ehail_fee
Trip_type
