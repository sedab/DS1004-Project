Code for generating visualizations included in Part 1 Report

### Generating Histograms 
Code used for yellow cap 

```./sql_query.sh --out_file group_by_pickup_date.out --query "SELECT hour(tpep_pickup_datetime), count(*) FROM yc GROUP BY hour(tpep_pickup_datetime) LIMIT 500"

./sql_query.sh --out_file group_by_dropoff_date.out --query "SELECT hour(tpep_dropoff_datetime), count(*) FROM yc GROUP BY hour(tpep_dropoff_datetime) LIMIT 500"

./sql_query.sh --out_file group_by_passenger_count.out --query "SELECT passenger_count, count(*) AS cts FROM yc GROUP BY passenger_count LIMIT 500"

./sql_query.sh --out_file group_by_payment_type.out --query "SELECT payment_type, count(*) AS cts FROM yc GROUP BY payment_type LIMIT 500"
 
./sql_query.sh --out_file group_by_mta_tax.out --query "SELECT mta_tax, count(*) AS cts FROM yc GROUP BY mta_tax LIMIT 500"

```

 Code used for green cap 

```./sql_query2.sh --out_file pickup_gc.out --query "SELECT hour(lpep_pickup_datetime ), count(*) FROM gc GROUP BY hour(lpep_pickup_datetime ) LIMIT 500"

./sql_query2.sh --out_file dropoff_gc.out --query "SELECT hour(Lpep_dropoff_datetime), count(*) FROM gc GROUP BY hour(Lpep_dropoff_datetime) LIMIT 500"

./sql_query2.sh --out_file passenger_count_gc.out --query "SELECT Passenger_count , count(*) AS cts FROM gc GROUP BY Passenger_count LIMIT 500"

./sql_query2.sh --out_file payment_type_gc3.out --query "SELECT Payment_type , count(*) AS cts FROM gc GROUP BY Payment_type LIMIT 500"

./sql_query2.sh --out_file mta_tax_gc.out --query "SELECT MTA_tax , count(*) AS cts FROM gc GROUP BY MTA_tax LIMIT 500" 
```
