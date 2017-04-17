%this script is to visualize the TLC data for DSGA1004 Project 
%date:04/14/17  

clc
clear all 
close all


%import data 
gb_dropoff_date=importdata('group_by_dropoff_date.out');
gb_pickup_date=importdata('group_by_passenger_pickup_date.out');
gb_passenger_count=importdata('group_by_passenger_count.out');
gb_mta_tax=importdata('group_by_mta_tax.out');
gb_payment_type=importdata('group_by_payment_type.out');

gb_RateCodeID=importdata('RateCodeID4_yc.out');
gb_VendorID = importfile('VendorID6_yc.out', 1, 5);

gb_trip_distance=importdata('trip_distance5_yc.out');
gb_fare_amount=importdata('fare_amount_yc.out');
gb_extra=importdata('extra_yc.out');
gb_tip_amount=importdata('tip_amount_yc.out');
gb_tolls_amount=importdata('tolls_amount_yc.out');
gb_improvement_surcharge=importdata('improvement_surcharge_yc.out');
gb_total_amount=importdata('total_amount_yc.out');

 
gb_pickup_month_yc=importdata('gb_pickup_month4_yc.out');
gb_dropoff_month_yc=importdata('gb_dropoff_month_yc.out');
gb_pickup_day_yc=importdata('gb_pickup_day_yc.out');
gb_dropoff_day_yc=importdata('gb_dropoff_day_yc.out');




  

%imort green cap 

gb_pickup_month_gc=importdata('gb_pickup_month_gc.out');
gb_dropoff_month_gc=importdata('gb_dropoff_month_gc.out');
gb_pickup_day_gc=importdata('gb_pickup_day_gc.out');
gb_dropoff_day_gc=importdata('gb_dropoff_day_gc.out'); 


gb_dropoff_gc=importdata('dropoff_gc.out'); 
gb_pickup_gc=importdata('pickup_gc.out');
gb_passenger_count_gc=importdata('passenger_count2_gc.out');
gb_mta_tax_gc=importdata('mta_tax_gc.out');
gb_payment_type_gc=importdata('payment_type_gc3.out');

gb_Trip_distance_gc=importdata('Trip_distance_gc.out');
gb_Fare_amount_gc=importdata('Fare_amount_gc.out');
gb_Extra_gc=importdata('Extra_gc.out');
gb_Tip_amount_gc=importdata('Tip_amount2_gc.out');
gb_Tolls_amount_gc=importdata('Tolls_amount_gc.out');
gb_Ehail_fee_gc=importdata('Ehail_fee3_gc.out');
gb_improvement_surcharge_gc=importdata('improvement_surcharge2_gc.out');
gb_Total_amount_gc=importdata('Total_amount2_gc.out');

gb_Trip_type_gc=importdata('Trip_type2_gc.out');
gb_RateCodeID_gc=importdata('RateCodeID3_gc.out');
gb_VendorID_gc=importdata('VendorID_gc.out');

%% 

 

%other data analysis 
 

query3_g=importdata('query3_g.out');
query3_y = importfile1('query3_y.out', 2, 73);
query4_g=importdata('query4_g.out');
query4_y=importdata('query4_y.out');



%visualize   

%% group by dropoff hour

figure(1)
[Y,I]=sort(gb_dropoff_date(:,1)); 
[Y2,I2]=sort(gb_dropoff_gc(:,1));

bar(gb_dropoff_date(I,1),gb_dropoff_date(I,2),'y') 
hold on
bar(gb_dropoff_gc(I2,1),gb_dropoff_gc(I2,2),'g') 
xlim([0 24])
xlabel('hours')
ylabel('count of taxi rides')
title('Yellow & Green Cab Dropoff Time Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')
%% group by pickup hour

figure(2)
[Y,I]=sort(gb_pickup_date(:,1)); 
[Y2,I2]=sort(gb_pickup_gc(:,1));

bar(gb_pickup_date(I,1),gb_pickup_date(I,2),'y')  
hold on 
bar(gb_pickup_gc(I2,1),gb_pickup_gc(I2,2),'g') 

xlim([0 24])
xlabel('hours')
ylabel('count of taxi rides')
title('Yellow & Green Cab Pickup Time Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')


 
%% group by passenger count 

figure(3)
[Y,I]=sort(gb_passenger_count(:,1)); 
[Y2,I2]=sort(gb_passenger_count_gc(:,1));

bar(gb_passenger_count(I,2),'y') 
hold on 
bar(gb_passenger_count_gc(I2,2),'g') 

set(gca,'XTickLabel',gb_passenger_count(I,1))
xlabel('passenger count')
ylabel('count of passenger count')
title('Yellow Taxi Passenger Count Histogram') 
legend('yellow cab', 'green cab')

clear Y I Y2 I2

%% group by mta_tax - green cab

figure(4)
[Y,I]=sort(gb_mta_tax_gc(:,2));

bar(gb_mta_tax_gc(I(10:end),2),'g')  
set(gca,'XTickLabel',gb_mta_tax_gc(I(10:end),1)) 
%ylim([0 605176710])
xlabel('mta tax')
ylabel('count of mta tax')
title('Green Taxi Mta Tax Histogram')
clear Y I
%% %% group by mta tax - yellow cab

figure(5)
[Y,I]=sort(gb_mta_tax(:,2));

bar(gb_mta_tax(I(195:end),2),'y')  
set(gca,'XTickLabel',{gb_mta_tax(I(195:end),1)}) 
ylim([0 605176710])
xlabel('mta tax')
ylabel('count of mta tax')
title('Yellow Taxi Mta Tax Histogram')
clear Y I

%% group by payment type - yellow 

%mapping
%1= Credit card -'CRD'
%2= Cash -'CSH'
%3= No charge -'NOC'
%4= Dispute -'DIS'
%5= Unknown - 'UNK'
%6= Voided trip - ''

namings={'CRD','CSH','NOC','DIS','UNK'}; 
counts=[gb_payment_type.data(4)+gb_payment_type.data(9),gb_payment_type.data(6)+gb_payment_type.data(7),gb_payment_type.data(5)+gb_payment_type.data(3),gb_payment_type.data(8)+gb_payment_type.data(10),gb_payment_type.data(1)+gb_payment_type.data(1)];
figure(6)
bar(counts, 'y')  
set(gca,'XTickLabel',namings)
xlabel('payment type')
ylabel('count of taxi rides')
title('Yellow Taxi Payment Type Histogram')
clear Y I
 

%% group by payment type -green cap

figure(7) 
bar(gb_payment_type_gc(:,2),'g')  
set(gca,'XTickLabel',gb_payment_type_gc(:,1))
xlabel('payment type')
ylabel('count of payment type')
title('Green Taxi Payment Type Histogram')
clear Y I

%% Group by RateCodeID 
figure(8)

[Y,I]=sort(gb_RateCodeID(:,1)); 
[Y2,I2]=sort(gb_RateCodeID_gc(:,1));

bar(gb_RateCodeID(I,1),gb_RateCodeID(I,2),'y') 
hold on
bar(gb_RateCodeID_gc(I2,1),gb_RateCodeID_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('RateCodeID')
ylabel('count of RateCodeIDs')
title('Yellow & Green Cab RateCodeID Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')
%% Group by VendorID 
figure(9)

[Y,I]=sort(gb_VendorID(:,1)); 
[Y2,I2]=sort(gb_VendorID_gc(:,1));

bar(gb_VendorID(I,1),gb_VendorID(I,2),'y') 
hold on
bar(gb_VendorID_gc(I2,1),gb_VendorID_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('VendorID')
ylabel('count of VendorIDs') 
title('Yellow & Green Cab VendorID Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab') 

%% 
%% Group by VendorID 
figure(9)

bar(cell2mat(gb_VendorID(:,2)),'y')  
hold on  
bar([gb_VendorID_gc(1,2),0,0,gb_VendorID_gc(2,2),0],'g')  
set(gca,'XTickLabel',gb_VendorID(:,1)) 

hold on
%xlim([0 24]) 
xlabel('VendorID')
ylabel('count of VendorIDs')  
title('Yellow & Green Cab VendorID Histogram')
%clear Y I Y2 I2 
legend('yellow cab', 'green cab') 

%% Group by Trip Type - green cap
figure(10)

[Y,I]=sort(gb_Trip_type_gc(:,1)); 

bar(gb_Trip_type_gc(I,2),'g')  
set(gca,'XTickLabel',gb_Trip_type_gc(I,1)) 
%xlim([0 24]) 
xlabel('Trip Type')
ylabel('count of Trip Types')
title('Green Cab trip Type Histogram')
clear Y I Y2 I2 
legend('green cab')  


%% group by trip distance - yellow

figure(11)

[Y,I]=sort(gb_trip_distance(:,1)); 
bar(gb_trip_distance(I,1),gb_trip_distance(I,2),'y') 
%xlim([0 24]) 
ylim([0 517375138])
xlabel('Trip Distance')
title('Yellow Taxi trip Distance Histogram')
clear Y I Y2 I2 
%%  group by trip distance - green

figure(12)

[Y2,I2]=sort(gb_Trip_distance_gc(:,1));


bar(gb_Trip_distance_gc(I2,1),gb_Trip_distance_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('Trip Distance')
title('Green Cab trip Distance Histogram')
clear Y I Y2 I2 

%%  group by fare amount - green

figure(13)

[Y,I]=sort(gb_fare_amount(:,1)); 
[Y2,I2]=sort(gb_Fare_amount_gc(:,1));

bar(gb_fare_amount(I,1),gb_fare_amount(I,2),'y') 
hold on
bar(gb_Fare_amount_gc(I2,1),gb_Fare_amount_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('fare amount')
title('Yellow & Green Cab Fare Amount Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')

%% group by extra 

figure(14)

[Y,I]=sort(gb_extra(:,1)); 
[Y2,I2]=sort(gb_Extra_gc(:,1));

bar(gb_extra(I,1),gb_extra(I,2),'y') 
hold on
bar(gb_Extra_gc(I2,1),gb_Extra_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('extra')
title('Yellow & Green Cab Extra Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab') 


%% group by tip amount

figure(15)

[Y,I]=sort(gb_tip_amount(:,1)); 
[Y2,I2]=sort(gb_Tip_amount_gc(:,1));

bar(gb_tip_amount(I,1),gb_tip_amount(I,2),'y') 
hold on
bar(gb_Tip_amount_gc(I2,1),gb_Tip_amount_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('tip amount')
title('Yellow & Green Cab Tip Amount Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab') 

%% group by tolls amount


figure(17)

[Y,I]=sort(gb_tolls_amount(:,1)); 
[Y2,I2]=sort(gb_Tolls_amount_gc(:,1));

bar(gb_tolls_amount(I,1),gb_tolls_amount(I,2),'y') 
hold on
bar(gb_Tolls_amount_gc(I2,1),gb_Tolls_amount_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('toll amount')
title('Yellow & Green Cab Toll Amount Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab') 


%%  group by improvement surcharge
figure(18)

[Y,I]=sort(gb_improvement_surcharge(:,1)); 
[Y2,I2]=sort(gb_improvement_surcharge_gc(:,1));

bar(gb_improvement_surcharge(I,1),gb_improvement_surcharge(I,2),'y') 
hold on
bar(gb_improvement_surcharge_gc(I2,1),gb_improvement_surcharge_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('improvement surcharge')
title('Yellow & Green Cab improvement surcharge Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')  
%% group by total amount

figure(19)

[Y,I]=sort(gb_total_amount(:,1)); 
[Y2,I2]=sort(gb_Total_amount_gc(:,1));

bar(gb_total_amount(I,1),gb_total_amount(I,2),'y') 
hold on
bar(gb_Total_amount_gc(I2,1),gb_Total_amount_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('total amount')
title('Yellow & Green Cab total amount Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')  
  
%% group by pickup month  

figure(20)

[Y,I]=sort(gb_pickup_month_yc(:,1)); 
[Y2,I2]=sort(gb_pickup_month_gc(:,1));

bar(gb_pickup_month_yc(I,1),gb_pickup_month_yc(I,2),'y') 
hold on
bar(gb_pickup_month_gc(I2,1),gb_pickup_month_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('month')
title('Yellow & Green Cab pickup by month Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')   


%% group by dropoff month

figure(21)

[Y,I]=sort(gb_dropoff_month_yc(:,1)); 
[Y2,I2]=sort(gb_dropoff_month_gc(:,1));

bar(gb_dropoff_month_yc(I,1),gb_dropoff_month_yc(I,2),'y') 
hold on
bar(gb_dropoff_month_gc(I2,1),gb_dropoff_month_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('month')
title('Yellow & Green Cab dropoff by month Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')   



%% group by pickup day

figure(22)

[Y,I]=sort(gb_pickup_day_yc(:,1)); 
[Y2,I2]=sort(gb_pickup_day_gc(:,1));

bar(gb_pickup_day_yc(I,1),gb_pickup_day_yc(I,2),'y') 
hold on
bar(gb_pickup_day_gc(I2,1),gb_pickup_day_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('day of the month')
title('Yellow & Green pickup by day of the month Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')   
%% group by dropoff day

figure(23)

[Y,I]=sort(gb_dropoff_day_yc(:,1)); 
[Y2,I2]=sort(gb_dropoff_day_gc(:,1));

bar(gb_dropoff_day_yc(I,1),gb_dropoff_day_yc(I,2),'y') 
hold on
bar(gb_dropoff_day_gc(I2,1),gb_dropoff_day_gc(I2,2),'g') 
%xlim([0 24]) 
xlabel('day of the month')
title('Yellow & Green Cab dropoff by day Histogram')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')   

%%
%More Analysis
  
%payment type vs average total amount
figure(24)  
b=bar(horzcat(query4_y.data(:,1),query4_g.data(:,1))); 
b(1).EdgeColor = 'black';
b(2).EdgeColor = 'black';
b(1).FaceColor = 'yellow';
b(2).FaceColor = 'green'; 

set(gca,'XTickLabel',query4_y.textdata(2:end,1))
ylim([0 22])
xlabel('payment type')
ylabel('average total amount')
title('yellow & green cab payment type vs average total amount')
clear Y I Y2 I2 
legend('yellow cab', 'green cab')

%% %% 
%store and forward flag vs hour of the day, count and percentage 

figure(25)   
[Y,I]=sort(query3_g.data(1:24,1)); 
[Y2,I2]=sort(query3_g.data(25:48,1)); 


plotyy([1:24],query3_g.data(I,4),[1:24],query3_g.data(I2+24,4));   
xlim([1 24])

ylabel('percentage')
xlabel('hours of the day')
title('green cab hours of the day vs percentage for store and forward flag ')
legend('N percentage','Y percentage')


%%  %store and forward flag vs hour of the day, count and percentage --DOOOO-fix the import

figure(26)   
[Y,I]=sort(cell2mat(query3_y(1:24,2))); 
[Y2,I2]=sort(cell2mat(query3_y(25:48,2))); 
[Y3,I3]=sort(cell2mat(query3_y(49:72,2))); 


plot([1:24],cell2mat(query3_y(I,5)))  
hold on
plot([1:24],cell2mat(query3_y(I2,5)));    
hold on 
plot([1:24],cell2mat(query3_y(I3,5)));    

xlim([1 24])

ylabel('percentage')
xlabel('hours of the day')
title('Yellow Cab hours of the day vs percentage for store and forward flag ')
legend('empty flag' ,'N flag','Y flag') 

 
 




