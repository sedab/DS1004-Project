from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext, HiveContext
import sys
from csv import reader
import pyspark.sql.functions as F
from pyspark.sql.functions import hour, udf, year, col, date_format
from pyspark.sql.types import StringType, DateType
from datetime import datetime


if __name__ == "__main__":
    conf =SparkConf()
    conf.set("spark.rpc.askTimeout","1000000s")
    conf.set("spark.executor.heartbeatInterval","10000000s")
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    
    #Read and prepare the data
    ##YELLOW CAB    
    lines_y= sc.textFile(sys.argv[1], 1)
    lines_y = lines_y.mapPartitions(lambda l: reader(l))
    lines_y = lines_y.filter(lambda l: len(l) > 0).filter(lambda l: 'endor' not in str(l[0]))
    
    yellow_len_19 = lines_y.filter(lambda l: len(l)==19)
    yellow_len_18 = lines_y.filter(lambda l: len(l)==18)
    yellow_len_17 = yellow_len_19.filter(lambda l:  str(l[17])=='').filter(lambda l: str(l[18])=='')
    
    yellow_18_corrected = yellow_len_18.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], "null", x[17], "null", "null"])
    yellow_17_corrected = yellow_len_17.map(lambda x: [x[0], x[1], x[2], x[3], x[4], 'null', 'null', x[5], x[6], 'null', 'null', x[9], x[10], x[11], x[12], x[13], x[14], x[16], x[17], x[7], x[8]])
    yellow_19_corrected = yellow_len_19.filter(lambda x: str(x[18]) != '').map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], x[17], x[18], "null", "null"])

    cols_names_y = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',  'PULocationID', 'DOLocationID')
    trips_y = yellow_17_corrected.union(yellow_18_corrected)
    trips_y = trips_y.union(yellow_19_corrected)
    trips_y = trips_y.filter(lambda l:l[7] != '99')
    trips_y = trips_y.filter(lambda l:l[7] in ['1','2','3','4','5','6'])
    schema_sd = sqlContext.createDataFrame(trips_y, cols_names_y)
    schema_sd.registerTempTable("yc")
    
    
    def GetPaymentType(x):
        if x == '1' or x == 'CRD':
            return "Credit Card"
        elif x == '2' or x == "CSH":
            return "Cash"
        elif x == '3' or x == "NOC":
            return "No Charge"
        elif x == '4' or x == "DIS":
            return "Dispute"
        elif x == '5' or x == "UNK":
            return "Unknown"
        elif x == '6':
            return "Voided Trip"
        else:
            return "N/A"
    
    def GetRateCode(x):
        if x == '1':
            return "Standard Rate"
        elif x == '2':
            return "JFK"
        elif x == '3':
            return "Newark"
        elif x == '4':
            return "Nassau or Westchester"
        elif x == '5':
            return "Negotiated fare"
        elif x == '6':
            return "Group ride"
        else:
            return "N/A"
    
    def GetDistance(x):
        x = float(x)
        if x < 1:
            return "<1"            
        elif x >= 1 and x < 2:
            return "1-2"
        elif x >= 2 and x < 3:
            return "2-3"
        elif x >= 3 and x < 4:
            return "3-4"
        elif x >= 4 and x < 5:
            return "4-5"
        elif x >= 5 and x < 6:
            return "5-6"
        elif x >= 6 and x < 7:
            return "6-7"
        elif x >= 7 and x < 8:
            return "7-8"
        elif x >= 8 and x < 9:
            return "8-9"
        elif x >= 9 and x < 10:
            return "9-10"
        else:
            return "10+"
    
    def GetTripType(x):
        if x == '1':
            return "Street Hail"
        elif x == '2':
            return "Dispatch"
        else:
            return "N/A"
    
    
    total=schema_sd.count()
    perc = trips_y.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], x[17], x[18], x[19], x[20],total])
    col_names_perc = ('VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',  'PULocationID', 'DOLocationID','total')

    df = sqlContext.createDataFrame(perc, col_names_perc)
    GetPaymentTypeUDF = udf(GetPaymentType, StringType())
    df2 = df.withColumn('payment_type_cat',GetPaymentTypeUDF(df['payment_type']))
    GetRateCodeUDF = udf(GetRateCode, StringType())    
    df3 = df2.withColumn('rate_code_cat',GetRateCodeUDF(df['RateCodeID']))       
    GetDistanceUDF = udf(GetDistance, StringType())
    df4 = df3.withColumn('distance_cat',GetDistanceUDF(df['trip_distance']))
    
    df4.createOrReplaceTempView("yc_all")
    
    ##GREEN CAB
    green_columns = ['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime',  'Store_and_fwd_flag', 'RateCodeID', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude', 'Passenger_count', 'Trip_distance', 'Fare_amount','Extra','MTA_tax', 'Tip_amount', 'Tolls_amount', 'Ehail_fee', 'improvement_surcharge', 'Total_amount', 'Payment_type', 'Trip_type',  'PULocationID', 'DOLocationID']
    green_data = sc.textFile(sys.argv[2], 1)
    broken_green = green_data.mapPartitions(lambda x: reader(x))
    broken_green_no_labels = broken_green.filter(lambda x: len(x) > 0).filter(lambda x: 'endor' not in x[0])
    gd_2013 = broken_green_no_labels.filter(lambda x: '2013' in x[1])
    gd_2014 = broken_green_no_labels.filter(lambda x: '2014' in x[1])
    gd_2015 = broken_green_no_labels.filter(lambda x: '2015' in x[1])
    gd_2016 = broken_green_no_labels.filter(lambda x: '2016' in x[1])
    gd_2016_1_6 = gd_2016.filter(lambda x: int(x[1].split("-")[1])<7)
    gd_2016_7_12 = gd_2016.filter(lambda x: int(x[1].split("-")[1])>6)
    
    fixed_2013 = gd_2013.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], "null", x[17], x[18], x[19],"null","null"])
    
    fixed_2014 = gd_2014.map(lambda x: [x[0], x[1] ,x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], "null", x[17], x[18], x[19],"null","null"])
    
    fixed_2015 = gd_2015.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],"null","null"])
    
    fixed_2016_1_6 = gd_2016_1_6.map(lambda x: [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],"null","null"])
    fixed_2016_7_12 = gd_2016_7_12.map(lambda x: [x[0],x[1],x[2],x[3],x[4],"null","null","null","null",x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[5],x[6]])
    
    total_data = fixed_2013.union(fixed_2014)
    total_data = total_data.union(fixed_2016_1_6)
    total_data = total_data.union(fixed_2016_7_12)
    total_data = total_data.union(fixed_2015)
    total_data = total_data.filter(lambda l:l[4] != '99')
    total_data = total_data.filter(lambda l:l[4] in ['1','2','3','4','5','6'])
    schema_sd_g = sqlContext.createDataFrame(total_data, green_columns)
    schema_sd_g.createOrReplaceTempView("gc")    
   
    total_g=schema_sd_g.count()
    perc_g = total_data.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13],x[14],x[15],x[16], x[17], x[18], x[19], x[20],x[21],x[22],total_g])
    col_names_perc_g = ['VendorID', 'lpep_pickup_datetime', 'Lpep_dropoff_datetime',  'Store_and_fwd_flag', 'RateCodeID', 'Pickup_longitude', 'Pickup_latitude', 'Dropoff_longitude', 'Dropoff_latitude', 'Passenger_count', 'Trip_distance', 'Fare_amount','Extra','MTA_tax', 'Tip_amount', 'Tolls_amount', 'Ehail_fee', 'improvement_surcharge', 'Total_amount', 'Payment_type', 'Trip_type',  'PULocationID', 'DOLocationID','total'] 

    df_g = sqlContext.createDataFrame(perc_g, col_names_perc_g)
    df2_g = df_g.withColumn('payment_type_cat',GetPaymentTypeUDF(df_g['Payment_type']))
    df3_g= df2_g.withColumn('rate_code_cat',GetRateCodeUDF(df_g['RateCodeID']))                
    df4_g = df3_g.withColumn('distance_cat',GetDistanceUDF(df_g['Trip_distance']))
    GetTripTypeUDF = udf(GetTripType, StringType()) 
    df5_g = df4_g.withColumn('trip_type_cat',GetTripTypeUDF(df_g['Trip_type']))
    df5_g.createOrReplaceTempView("gc_all")
   
    #QUERIES   
    ## 1. Counts of Payment Types per vendor  ---- READY WITH OUTPUT FOR BOTH
    ###Yellow Cab
    '''PaymentTypesQry = sqlContext.sql("select payment_type_cat, \
                                        CASE   WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC' WHEN VendorID = 2 THEN 'VeriFone Inc.' ELSE VendorID END as VendorID, \
                                        count(*) as cnt from yc_all group by payment_type_cat, VendorID ORDER BY payment_type_cat")    
    
    PaymentTypesInfo = PaymentTypesQry.rdd.map(lambda p: str(p.payment_type_cat) + "\t" + str(p.VendorID) + "\t" + str(p.cnt))
    header = sc.parallelize(["Payment Type\tVendor ID\tCount"])
    PaymentTypesInfo = header.union(PaymentTypesInfo)
    PaymentTypesInfo.saveAsTextFile("query1_y.out")

    ###Green Cab
    q1_g = sqlContext.sql("select payment_type_cat, \
                                        CASE   WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC' WHEN VendorID = 2 THEN 'VeriFone Inc.' ELSE VendorID END as VendorID, \
                                        count(*) as cnt from gc_all group by payment_type_cat, VendorID ORDER BY payment_type_cat")    
    
    q1_g_info = q1_g.rdd.map(lambda p: str(p.payment_type_cat) + "\t" + str(p.VendorID) + "\t" + str(p.cnt))
    header = sc.parallelize(["Payment Type\tVendor ID\tCount"])
    q1_g_info = header.union(q1_g_info)
    q1_g_info.saveAsTextFile("query1_g.out")'''
    
    ## 2. Percentage of the different Rate Codes by time ---- READY WITH OUTPUT FOR BOTH
    ###Yellow Cab
    '''RateCodePerc = sqlContext.sql("select rate_code_cat, count(1) as cnt, total, count(1)*100/total as perc, hour(tpep_pickup_datetime) as timeofday from yc_all group by rate_code_cat, total, hour(tpep_pickup_datetime) order by rate_code_cat")
    
    RatePercInfo = RateCodePerc.rdd.map(lambda p: str(p.rate_code_cat) + "\t" + str(p.timeofday) + "\t" + str(p.cnt) + "\t" + str(p.total) + "\t" + str(p.perc))
    header3 = sc.parallelize(["Rate Code\tTime of Day\tCount\tTotal\tPercentage"])
    RatePercInfo = header3.union(RatePercInfo)
    RatePercInfo.saveAsTextFile("query2_y.out")
    
    ###Green Cab
    q2_g = sqlContext.sql("SELECT rate_code_cat,count(1) as cnt, total ,count(1)*100/total as perc,hour(lpep_pickup_datetime) as timeofday FROM gc_all GROUP BY rate_code_cat,total, hour(lpep_pickup_datetime) ORDER BY rate_code_cat")

    q2_g_info = q2_g.rdd.map(lambda p: str(p.rate_code_cat) + "\t" + str(p.timeofday) + "\t" + str(p.cnt) + "\t" + str(p.total) + "\t" + str(p.perc))
    header3 = sc.parallelize(["Rate Code\tTime of Day\tCount\tTotal\tPercentage"])
    q2_g_info = header3.union(q2_g_info)
    q2_g_info.saveAsTextFile("query2_g.out")'''
    
    ## 3. Percentage of store and forward flags by time ---- READY WITH OUTPUT FOR BOTH
    ###Yellow Cab
    '''SandF = sqlContext.sql("SELECT  store_and_fwd_flag, count(1) as cnt, total,count(1)*100/total as perc,hour(tpep_pickup_datetime) as timeofday FROM yc_all GROUP BY store_and_fwd_flag, total, hour(tpep_pickup_datetime) ORDER BY store_and_fwd_flag")
    
    SandFInfo = SandF.rdd.map(lambda p: str(p.store_and_fwd_flag) + "\t" + str(p.timeofday) + "\t" + str(p.cnt) + "\t" + str(p.total) + "\t" + str(p.perc))
    header3 = sc.parallelize(["Store and Forward Flag\tHour of Day\tCount\tTotal\tPercentage"])
    SandFInfo = header3.union(SandFInfo)
    SandFInfo.saveAsTextFile("query3_y.out")
    
    ###Green Cab
    q3_g = sqlContext.sql("SELECT  store_and_fwd_flag, count(1) as cnt, total,count(1)*100/total as perc,hour(lpep_pickup_datetime) as timeofday FROM gc_all GROUP BY store_and_fwd_flag, total, hour(lpep_pickup_datetime) ORDER BY store_and_fwd_flag")
    
    q3_g_info = q3_g.rdd.map(lambda p: str(p.store_and_fwd_flag) + "\t" + str(p.timeofday) + "\t" + str(p.cnt) + "\t" + str(p.total) + "\t" + str(p.perc))
    header3 = sc.parallelize(["Store and Forward Flag\tHour of Day\tCount\tTotal\tPercentage"])
    q3_g_info = header3.union(q3_g_info)
    q3_g_info.saveAsTextFile("query3_g.out")'''
        
    ## 4. Average total amount per payment type ---- READY WITH OUTPUT FOR BOTH
    ###Yellow Cab
    '''TotPerPayType = sqlContext.sql("SELECT payment_type_cat, count(1) as cnt, AVG(total_amount) as avg_total_amount from yc_all GROUP BY payment_type_cat")
    
    TotPerPayTypeInfo = TotPerPayType.rdd.map(lambda p: str(p.payment_type_cat) + "\t" + str(p.avg_total_amount) + "\t" + str(p.cnt))
    header4 = sc.parallelize(["Payment Type\tAverage Total Amount\tCount"])
    TotPerPayTypeInfo = header4.union(TotPerPayTypeInfo)
    TotPerPayTypeInfo.saveAsTextFile("query4_y.out")
    
    q4_g = sqlContext.sql("SELECT payment_type_cat, count(1) as cnt, AVG(Total_amount) as avg_total_amount from gc_all GROUP BY payment_type_cat")
    
    q4_g_info = q4_g.rdd.map(lambda p: str(p.payment_type_cat) + "\t" + str(p.avg_total_amount) + "\t" + str(p.cnt))
    header4 = sc.parallelize(["Payment Type\tAverage Total Amount\tCount"])
    q4_g_info = header4.union(q4_g_info)
    q4_g_info.saveAsTextFile("query4_g.out")'''
    
    ## 5. Average passenger count per time of day ---- 
    ###Yellow Cab
    '''PsgrCount = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as timeofday, AVG(passenger_count) as avg_passenger_count from yc_all GROUP BY hour(tpep_pickup_datetime)")
    
    PsgrCountInfo = PsgrCount.rdd.map(lambda p: str(p.timeofday)+"\t" + str(p.avg_passenger_count))
    header5 = sc.parallelize(["Hour of Day\tAverage Passenger Count"])
    PsgrCountInfo = header5.union(PsgrCountInfo)
    PsgrCountInfo.saveAsTextFile("query5_y.out")
    
    ###Green Cab
    q5_g = sqlContext.sql("SELECT hour(lpep_pickup_datetime) as timeofday, AVG(Passenger_count) as avg_passenger_count from gc_all GROUP BY hour(lpep_pickup_datetime)")
    
    q5_g_info = q5_g.rdd.map(lambda p: str(p.timeofday)+"\t" + str(p.avg_passenger_count))
    header5 = sc.parallelize(["Hour of Day\tAverage Passenger Count"])
    q5_g_info = header5.union(q5_g_info)
    q5_g_info.saveAsTextFile("query5_g.out")'''
    
    
    ## 6. Number of Daily Trips  ----
    ###Yellow Cab
    '''DailyTrips = sqlContext.sql("SELECT to_date(tpep_pickup_datetime) as pickup_date, count(*) num_trips FROM yc_all GROUP BY to_date(tpep_pickup_datetime)")
    
    DailyTripsInfo = DailyTrips.rdd.map(lambda p: str(p.pickup_date)+"\t" + str(p.num_trips))
    header7 = sc.parallelize(["Pickup Date\tNumber of Trips"])
    DailyTripsInfo = header7.union(DailyTripsInfo)
    DailyTripsInfo.saveAsTextFile("query6_y.out")
    
    ###Green Cab
    q6_g = sqlContext.sql("SELECT to_date(lpep_pickup_datetime) as pickup_date, count(*) num_trips FROM gc_all GROUP BY to_date(lpep_pickup_datetime)")
    
    q6_g_info = q6_g.rdd.map(lambda p: str(p.pickup_date)+"\t" + str(p.num_trips))
    header7 = sc.parallelize(["Pickup Date\tNumber of Trips"])
    q6_g_info = header7.union(q6_g_info)
    q6_g_info.saveAsTextFile("query6_g.out")'''
    
    
    ## 7. Percentage of trip distances per year
    
    '''TripDistances = sqlContext.sql("SELECT distance_cat, year(tpep_pickup_datetime) as pickup_year, count(1) as cnt, t.total_per_year,count(1)*100/t.total_per_year as perc FROM yc_all, (select year(tpep_pickup_datetime) as pickup_year2, count(1) as total_per_year from yc_all group by year(tpep_pickup_datetime)) as t WHERE year(tpep_pickup_datetime) = t.pickup_year2 GROUP BY distance_cat,year(tpep_pickup_datetime),t.total_per_year ORDER BY year(tpep_pickup_datetime)")
    
    TripDistancesInfo = TripDistances.rdd.map(lambda p: str(p.pickup_year)+"\t" + str(p.distance_cat) + "\t" + str(p.cnt) + "\t" + str(p.total_per_year) + "\t" + str(p.perc))
    header8 = sc.parallelize(["Year\tDistance\tCount\tTotal per Year\tPercentage"])
    TripDistancesInfo = header8.union(TripDistancesInfo)
    TripDistancesInfo.saveAsTextFile("query7_y.out")
    
    q7_g = sqlContext.sql("SELECT distance_cat, year(lpep_pickup_datetime) as pickup_year, count(1) as cnt, t.total_per_year,count(1)*100/t.total_per_year as perc FROM gc_all, (select year(lpep_pickup_datetime) as pickup_year2, count(1) as total_per_year from gc_all group by year(lpep_pickup_datetime)) as t WHERE year(lpep_pickup_datetime) = t.pickup_year2 GROUP BY distance_cat,year(lpep_pickup_datetime),t.total_per_year ORDER BY year(lpep_pickup_datetime)")
    
    q7_g_info = q7_g.rdd.map(lambda p: str(p.pickup_year)+"\t" + str(p.distance_cat) + "\t" + str(p.cnt) + "\t" + str(p.total_per_year) + "\t" + str(p.perc))
    header8 = sc.parallelize(["Year\tDistance\tCount\tTotal per Year\tPercentage"])
    q7_g_info = header8.union(q7_g_info)
    q7_g_info.saveAsTextFile("query7_g.out")'''
    
    
    ## 8. Percentage of trip types per rate code (trip type applies to green cabs only)
    '''q8_g = sqlContext.sql("SELECT trip_type_cat, count(1) as cnt, total,count(1)*100/total as perc, rate_code_cat FROM gc_all GROUP BY trip_type_cat, total, rate_code_cat ORDER BY trip_type_cat")
    
    q8_g_info = q8_g.rdd.map(lambda p: str(p.trip_type_cat)+ "\t" + str(p.rate_code_cat) + "\t" + str(p.cnt) + "\t" + str(p.total) + "\t" + str(p.perc))
    header8 = sc.parallelize(["Trip Type\tRate Code\tCount\tTotal\tPercentage"])
    q8_g_info = header8.union(q8_g_info)
    q8_g_info.saveAsTextFile("query8_g.out")'''
    
    #Hypothesis
    hourly_weather_data = sc.textFile(sys.argv[3], 1)
    broken_hours = hourly_weather_data.mapPartitions(lambda x: reader(x))
    weather_columns =broken_hours.take(1)[0]
    weather_data_no_label = broken_hours.filter(lambda x: x[1] != 'STATION_NAME')
    schema_sd = sqlContext.createDataFrame(weather_data_no_label, weather_columns)
    
    def FormatDate(date):
        date = str(date)        
        return datetime.strptime(date,'%m/%d/%y').strftime('%Y-%m-%d')
    
    
    
    
    schema_sd.createOrReplaceTempView("wd")
    
    ## 0. Get weather parameters per date
    '''query_res = sqlContext.sql("select split(DATE, ' ')[0] as day_of, avg(DAILYPrecip) as DailyPrecip, avg(DAILYSnowfall) as DAILYSnowfall, avg(DAILYAverageWindSpeed) as DAILYAverageWindSpeed, avg(DAILYPeakWindSpeed) as DAILYPeakWindSpeed, avg(HOURLYDRYBULBTEMPF) as temp from wd group by split(DATE, ' ')[0]")
    query_res_rdd = query_res.rdd.map(lambda x: str(x.day_of) + "\t" +str(x.DailyPrecip)+'\t'+ str(x.DAILYSnowfall) + '\t'+ str(x.DAILYAverageWindSpeed) + '\t'+ str(x.DAILYPeakWindSpeed) + '\t' + str(x.temp))
    header = sc.parallelize(["Date\tDaily Precipitation\tDaily Snow Fall\tDaily Average Wind Speed\tDaily Peak Wind Speed\tTemperature"])
    header.union(query_res_rdd).saveAsTextFile("hypo1.out")'''
    
    ## 1. Drop in yellow taxi rides on 2/22/2014 and 1/23/2016 is due to bad weather.
   
    
    '''HourlyTrips = sqlContext.sql("SELECT hour(tpep_pickup_datetime) as pickup_date, count(*) num_trips FROM yc_all where to_date(tpep_pickup_datetime) = to_date('2014-02-22') GROUP BY hour(tpep_pickup_datetime)")
    HourlyTripsInfo = HourlyTrips.rdd.map(lambda p: str(p.pickup_date)+"\t" + str(p.num_trips))
    header7 = sc.parallelize(["Pickup Hour\tNumber of Trips"])
    HourlyTripsInfo = header7.union(HourlyTripsInfo)
    HourlyTripsInfo.saveAsTextFile("hypo1b.out")'''
    
    
    ## 2. Increase in tip and total amounts due to bad weather.
    '''Amounts = sqlContext.sql("SELECT to_date(tpep_pickup_datetime) as pickup_date, avg(tip_amount) avg_tip, avg(total_amount) as avg_total FROM yc_all GROUP BY to_date(tpep_pickup_datetime)")
    AmountsInfo = Amounts.rdd.map(lambda p: str(p.pickup_date)+"\t" + str(p.avg_tip) + "\t" + str(p.avg_total))
    header8 = sc.parallelize(["Pickup Date\tAverage Tip Amount\tAverage Total Amount"])
    AmountsInfo = header8.union(AmountsInfo)
    AmountsInfo.saveAsTextFile("hypo2.out")'''

    
    sc.stop()