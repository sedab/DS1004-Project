Code for generating visualizations included in Part 1 Report

### Generating Histograms 
Code used for yellow cap 

```
./sql_query.sh --out_file group_by_pickup_date.out --query "SELECT hour(tpep_pickup_datetime), count(*) FROM yc GROUP BY hour(tpep_pickup_datetime) LIMIT 500"

./sql_query.sh --out_file group_by_dropoff_date.out --query "SELECT hour(tpep_dropoff_datetime), count(*) FROM yc GROUP BY hour(tpep_dropoff_datetime) LIMIT 500"

./sql_query.sh --out_file group_by_passenger_count.out --query "SELECT passenger_count, count(*) AS cts FROM yc GROUP BY passenger_count LIMIT 500"

./sql_query.sh --out_file group_by_payment_type.out --query "SELECT payment_type, count(*) AS cts FROM yc GROUP BY payment_type LIMIT 500"
 
./sql_query.sh --out_file group_by_mta_tax.out --query "SELECT mta_tax, count(*) AS cts FROM yc GROUP BY mta_tax LIMIT 500" 

./sql_query.sh --out_file RateCodeID4_yc.out --query "SELECT RateCodeID , count(*) AS cts FROM yc GROUP BY RateCodeID LIMIT 500"

./sql_query.sh --out_file VendorID6_yc.out --query "SELECT VendorID , count(*) AS cts FROM yc GROUP BY VendorID LIMIT 500" 

./sql_query.sh --out_file trip_distance4_yc.out --query "select floor(trip_distance/1000.00)*1000 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file fare_amount_yc.out --query "select floor(fare_amount/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file extra_yc.out --query "select floor(extra/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file tip_amount_yc.out --query "select floor(tip_amount/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file tolls_amount_yc.out --query "select floor(tolls_amount/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file improvement_surcharge_yc.out --query "select floor(improvement_surcharge/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"

./sql_query.sh --out_file total_amount_yc.out --query "select floor(total_amount/5.00)*5 as bucket_floor , count(*) as count from yc group by 1 order by 1 LIMIT 500"  

./sql_query.sh --out_file gb_pickup_month4_yc.out --query "SELECT MONTH(tpep_pickup_datetime), count(*) FROM yc GROUP BY MONTH(tpep_pickup_datetime) LIMIT 500"  

./sql_query.sh --out_file gb_dropoff_month_yc.out --query "SELECT MONTH(tpep_dropoff_datetime), count(*) FROM yc GROUP BY MONTH(tpep_dropoff_datetime) LIMIT 500"  

 ./sql_query.sh --out_file gb_pickup_day_yc.out --query "SELECT DAY(tpep_pickup_datetime), count(*) FROM yc GROUP BY DAY(tpep_pickup_datetime) LIMIT 500"  
 
 ./sql_query.sh --out_file gb_dropoff_day_yc.out --query "SELECT DAY(tpep_dropoff_datetime), count(*) FROM yc GROUP BY DAY(tpep_dropoff_datetime) LIMIT 500"

```

 Code used for green cap 

```
./sql_query2.sh --out_file pickup_gc.out --query "SELECT hour(lpep_pickup_datetime ), count(*) FROM gc GROUP BY hour(lpep_pickup_datetime ) LIMIT 500"

./sql_query2.sh --out_file dropoff_gc.out --query "SELECT hour(Lpep_dropoff_datetime), count(*) FROM gc GROUP BY hour(Lpep_dropoff_datetime) LIMIT 500"

./sql_query2.sh --out_file passenger_count2_gc.out --query "SELECT Passenger_count , count(*) AS cts FROM gc GROUP BY Passenger_count LIMIT 500"

./sql_query2.sh --out_file payment_type_gc3.out --query "SELECT Payment_type , count(*) AS cts FROM gc GROUP BY Payment_type LIMIT 500"

./sql_query2.sh --out_file mta_tax_gc.out --query "SELECT MTA_tax , count(*) AS cts FROM gc GROUP BY MTA_tax LIMIT 500" 

./sql_query2.sh --out_file Trip_type2_gc.out --query "SELECT Trip_type , count(*) AS cts FROM gc GROUP BY Trip_type LIMIT 500"

./sql_query2.sh --out_file RateCodeID3_gc.out --query "SELECT RateCodeID  , count(*) AS cts FROM gc GROUP BY RateCodeID  LIMIT 500"

./sql_query2.sh --out_file VendorID_gc.out --query "SELECT VendorID , count(*) AS cts FROM gc GROUP BY VendorID LIMIT 500" 

./sql_query2.sh --out_file Trip_distance_gc.out --query "select floor(Trip_distance/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Fare_amount_gc.out --query "select floor(Fare_amount/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Extra_gc.out --query "select floor(Extra/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Tip_amount2_gc.out --query "select floor(Tip_amount/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Tolls_amount_gc.out --query "select floor(Tolls_amount/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Ehail_fee3_gc.out --query "select floor(Ehail_fee/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file improvement_surcharge2_gc.out --query "select floor(improvement_surcharge/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500"

./sql_query2.sh --out_file Total_amount2_gc.out --query "select floor(Total_amount/5.00)*5 as bucket_floor , count(*) as count from gc group by 1 order by 1 LIMIT 500" 

./sql_query2.sh --out_file gb_dropoff_month_gc.out --query "SELECT MONTH(Lpep_dropoff_datetime), count(*) FROM gc GROUP BY MONTH(Lpep_dropoff_datetime) LIMIT 500" 

./sql_query2.sh --out_file gb_pickup_month_gc.out --query "SELECT MONTH(lpep_pickup_datetime ), count(*) FROM gc GROUP BY MONTH(lpep_pickup_datetime) LIMIT 500"

./sql_query2.sh --out_file gb_pickup_day_gc.out --query "SELECT DAY(lpep_pickup_datetime ), count(*) FROM gc GROUP BY DAY(lpep_pickup_datetime) LIMIT 500"

./sql_query2.sh --out_file gb_dropoff_day_gc.out --query "SELECT DAY(Lpep_dropoff_datetime), count(*) FROM gc GROUP BY DAY(Lpep_dropoff_datetime) LIMIT 500"

```
