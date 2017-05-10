#this script is to visualize the TLC data for DSGA1004 Project for Part-2
#date:05/01/17


#######################################import data################################################################

d1 <- read.csv2("green_cab_daily.out",sep = "\t")  

#d2 <- read.csv2("green_cabs_cost_discrepancy.out",sep = "\t") 

d3 <- read.csv2("green_cabs_dropoff_pickup_delta_with_years.out",sep = "\t") 
d4 <- read.csv2("green_cabs_dropoff_pickup_delta.out",sep = "\t") 
d5 <- read.csv2("hypo1.out",sep = "\t") 
d6 <- read.csv2("query6_y.out.csv",sep = "\t") 
d7 <- read.csv2("weather_data_by_day_avg_precip_no_T.out",sep = "\t")  
d8 <- read.csv2("weather_data_by_day_avg_snowfall_no_T.out",sep = "\t") 
d9 <- read.csv2("weather_data_by_day_avg_temp_no_T.out",sep = "\t") 
d10 <- read.csv2("weather_data_by_day_by_station_avg_precip_no_T.out",sep = "\t")  

#d11 <- read("weather_data_by_day_by_station_with_precip.out",sep = "\t") 
d12 <- read.csv2("weather_data_by_day_by_station.out",sep = "\t") 
d13 <- read.csv2("weather_data_by_day_max_precip.out",sep = "\t") 
d14 <- read.csv2("yellow_cabs_daily_nicer.out",sep = "\t") 
d15 <- read.csv2("yellow_cabs_dropoff_pickup_delta_with_years.out",sep = "\t") 
d16 <- read.csv2("yellow_cabs_dropoff_pickup_delta.out",sep = "\t") 

d17 <- read.csv2("hypo2.out",sep = "\t")  
 
d18 <- read.csv2("Monthly Regular Grade Motor Gasoline Prices.csv",sep = "\t")  
 
#new data 050517 

d19 <- read.csv2("green_cabs_hourly_rides_day_of_week.out",sep = "\t")  
d20 <- read.csv2("green_cabs_hourly_rides_day_of_week_by_date.out",sep = "\t")  
d21 <- read.csv2("green_cabs_hourly_rides_day_of_week_by_year.out",sep = "\t")  
d22 <- read.csv2("green_cabs_hourly_rides_feb_2014.out",sep = "\t")  
d23 <- read.csv2("yellow_cabs_hourly_rides_day_of_week.out",sep = "\t")  
d24 <- read.csv2("yellow_cabs_hourly_rides_day_of_week_by_date.out",sep = "\t")  
d25 <- read.csv2("yellow_cabs_hourly_rides_day_of_week_by_year.out",sep = "\t")  


#050617 

d27<- read.csv2("yellow_cabs_hourly_rides_feb_2014.out",sep = "\t")  

#######################################visualize data################################################################


#install the ggplot2 package:
install.packages("ggplot2")
#load it
library("ggplot2")

d1$Pickup_date=strptime(d1$Pickup_date,format='%Y-%m-%d') 
 
library(scales)

#green cab count of taxi pickups with dates 
ggplot(d1, aes(x=Pickup_date, y=Count )) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+ 
  geom_smooth(method = "loess", level=0.90, color = "red")+
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +
  ylab("Count")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Green cab pickup vs date")





 
#find outlier dates 
#a = boxplot(d1$Count) 
#a$out 

#... 

d9$Date=strptime(d9$Date,format='%m/%d/%y') 

ggplot(d9, aes(x=Date, y=AVG_daily_temp )) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+ 
  geom_smooth()+
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("AVG_daily_temp")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("...") 

 


   
d10$Date=strptime(d10$Date,format='%m/%d/%y') 

ggplot(d10, aes(x=Date, y=AVG_daily_precip, colour=station_name)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("AVG_daily_precip")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('...')


d12$Date=strptime(d12$Date,format='%m/%d/%y') 

ggplot(d12, aes(x=Date, y=Count, colour=Weather_Station)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("Count")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Not sure what the count is - weather_data_by_day_by_station.out')
  


d13$Date=strptime(d13$Date,format='%m/%d/%y') 
d13
ggplot(d13, aes(x=Date, y=max_daily_precip)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("max daily precip")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('there are values T - not sure what they are') 




d14$Pickup_date=strptime(d14$Pickup_date,format='%Y-%m-%d') 
d14
ggplot(d14, aes(x=Pickup_date, y=Count)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  geom_smooth()+
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("count")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab counts daily')






ggplot(d15, aes(x=Date_delta, y=Count)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date_delta") +
  ylab("count")+ 
  #theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('yellow_cabs_dropoff_pickup_delta_with_years')



ggplot(d16, aes(x=Date_delta, y=Count)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date_delta") +
  ylab("count")+ 
  #theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('yellow_cabs_dropoff_pickup_delta')




 
ggplot(d3, aes(x=Date_delta, y=Count)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date_delta") +
  ylab("count")+ 
  #theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('green_cabs_dropoff_pickup_delta_with_years')




ggplot(d4, aes(x=Date_delta, y=Count)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date_delta") +
  ylab("count")+ 
  #theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('green_cabs_dropoff_pickup_delta')




library(reshape)

d5$Date=strptime(d5$Date,format='%m/%d/%Y')  
d5_m <- melt(d5, id=c("Date")) 
d5_m$Date=strptime(d5_m$Date,format='%Y/%m/%d')  


ggplot(d5_m, aes(x=Date, y=value, colour=variable)) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('weather')  
 
 

d6_u <- data.frame(do.call('rbind', strsplit(as.character(d6$Pickup.Date.Number.of.Trips),',',fixed=TRUE)))
d6_u$X1=strptime(d6_u$X1,format='%m/%d/%Y')  

ggplot(d6_u, aes(x=X1, y=as.numeric(X2))) + 
  #geom_bar(position=position_dodge(), stat="identity")+ 
  geom_point()+  
  geom_smooth()+
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("calender day") +
  ylab("number of trips")+  
  #scale_y_continuous(limit = c(0, 6000))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('yellow cab number of trips per day') 




d7$Date=strptime(d7$Date,format='%m/%d/%y')  
d7_m <- melt(d7, id=c("Date")) 

ggplot(d7, aes(x=Date, y=AVG_daily_precip)) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  #ylab("")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('dont know what is the count column') 

  

 
d8$Date=strptime(d8$Date,format='%m/%d/%y')  
d8_m <- melt(d8, id=c("Date")) 

ggplot(d8, aes(x=Date, y=AVG_daily_snowfallp)) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  #ylab("")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Average daily snowdall by date')  

 

d9$Date=strptime(d9$Date,format='%m/%d/%y')  
d9_m <- melt(d9, id=c("Date"))
ggplot(d9, aes(x=Date, y=AVG_daily_temp)) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  #ylab("")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('dont know what is the count column')   

  
 
#######average tip amount and total amount vs Date############# 

d17$Pickup.Date=strptime(d17$Pickup.Date,format='%m/%d/%Y')  
d17$day <- weekdays(as.Date(d17$Pickup.Date))
 
ggplot(d17, aes(x=Pickup.Date, y=as.numeric(Average.Tip.Amount), colour=day)) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +  
  geom_smooth()+
  ylab("average tip amount")+
  scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Average Tip Amount vs Dates')  
  
ggplot(d17, aes(x=Pickup.Date, y=as.numeric(Average.Tip.Amount))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +  
  geom_smooth()+
  ylab("average tip amount")+
  scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Average Tip Amount vs Dates')  

ggplot(d17[1:1065,], aes(x=Pickup.Date, y=as.numeric(Average.Total.Amount))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("average total amount")+  
  geom_smooth()+  
  #geom_line(data=d18$price,aes(color="Second line"))+
  scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Average Total Amount vs Dates')    

ggplot(d17[1:1065,], aes(x=Pickup.Date, y=as.numeric(Average.Total.Amount), colour=day)) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("average total amount")+  
  geom_smooth()+
  scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('Average Total Amount vs Dates')


# d18 gasoline price
d18$date <-paste(d18$month, '1', d18$year,  sep = "/")
d18$da=strptime(d18$date,format='%m/%d/%Y')   


install.packages('lubridate')
library(lubridate)

 
for (i in 1:length(d17[,1])){  
  for(m in 1:length(d18[,1])){
  if (month(d17$Pickup.Date[i])==month(d18$da[m]) & year(d17$Pickup.Date[i])==year(d18$da[m])){
    d17$gas_price[i]=(as.numeric(as.character(d18$price[m]))*10)-2000
    #print(d18$price[[m]])
    }}}  
 



#plot the average tip amount, average total amount and gas price 
d17$Average.Tip.Amount= as.double(d17$Average.Tip.Amount)
d17$Average.Total.Amount=  as.double(d17$Average.Total.Amount) 
 

d17_mm <- melt(d17, id=c("Pickup.Date", "day","price","price2","price3")) 

ggplot(d17_mm, aes(x=Pickup.Date, y=as.numeric(value), color=variable)) + 
  #geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("....")+  
  geom_smooth()+
  #scale_y_continuous(limit = c(0, 9))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('Gas price vs average total amount & average tip amount')



#plot the average tip amount / total amount 


ggplot(d17[1:1065,], aes(x=Pickup.Date, y=as.numeric(Average.Tip.Amount)/as.numeric(Average.Total.Amount))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +
  ylab("average tip amount/ average total amount")+  
  geom_smooth()+
  scale_y_continuous(limit = c(0, 9))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  #theme(axis.text.y = element_blank())+
  ggtitle('generosity - average tip per total amount vs dates')



#d19 pickup hour& pickup date
 
ggplot(d19, aes(x=pickup_hour, y=Count, colour=Pickup_dayofweek)) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("hour") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  #theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab hourly rides day of the week')  





#d20 pickup hour& pickup date 
d20$da=strptime(d20$pickup_date,format='%Y-%m-%d')   

#pickup_day
ggplot(d20, aes(x=da, y=Count, colour=Pickup_dayofweek)) + 
  #geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("year") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab rides the day of the week by date')  


#pickup_hour
ggplot(d20, aes(x=da, y=Count, colour=factor(pickup_hour))) + 
  #geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("time") +  
  geom_smooth( )+
  ylab("count")+
  scale_y_continuous(limit = c(0, 6000))+
  theme(axis.text.x = element_text(size=10,angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab rides count, by the time and hour')  








#d21 green_cabs_hourly_rides_day_of_week_by_year
#exclude 2013 
d21=d21[!d21$pickup_year==2013,]
#pickup_day
ggplot(d21, aes(x=pickup_hour, y=Count, colour=factor(pickup_year))) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("hour") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab rides count by year and hour of the day')  



ggplot(d21, aes(x=pickup_hour, y=Count, colour=factor(Pickup_dayofweek))) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_hour") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab ride count by hour and the day of the week')  



#d22 green_cabs_hourly_rides_feb_2014 
d22$da=strptime(d22$Pickup_date,format='%Y-%m-%d')   

ggplot(d22, aes(x=da, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab ride count on feb_2014 , by pickup date and hour ')   




#d23 yellow_cabs_hourly_rides_day_of_week

ggplot(d23, aes(x=pickup_hour, y=Count, colour=factor(Pickup_dayofweek))) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_hour") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab ride count, by hour and by the day of the week')  


 


#d24 yellow_cabs_hourly_rides_day_of_week_by_date
d24$da=strptime(d24$pickup_date,format='%Y-%m-%d')   

#pickup_day
ggplot(d24, aes(x=da, y=Count, colour=Pickup_dayofweek)) + 
  #geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("date") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab rides count, by date and the day of the week')  


#pickup_hour
ggplot(d24, aes(x=da, y=Count, colour=factor(pickup_hour))) + 
  #geom_point()+  
  scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("time") +  
  geom_smooth( )+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 6000))+
  theme(axis.text.x = element_text(size=10,angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab rides count, by the date and hour')   


d25=d25[!d25$pickup_year==2013,]
#25 yellow_cabs_hourly_rides_day_of_week_by_year 
#pickup_day
ggplot(d25, aes(x=pickup_hour, y=Count, colour=factor(pickup_year))) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("hour") +  
  geom_smooth()+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab rides count by year and hour of the day')  



ggplot(d25, aes(x=pickup_hour, y=Count, colour=factor(Pickup_dayofweek))) + 
  geom_point()+  
  #scale_x_datetime(breaks=date_breaks("3 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_hour") +  
  geom_smooth(se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab ride count by hour and the day of the week') 




#050617 
d27$da=strptime(d27$Pickup_date,format='%Y-%m-%d')   

ggplot(d27, aes(x=da, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab ride count on feb_2014 , by pickup date and hour ')  

 




#050917 
#yearly volume by calender date 
#d1-green, d14-yellow 
d1$year=year(d1$Pickup_date) 
d14$year=year(d14$Pickup_date)
d1$month=month(d1$Pickup_date) 
d14$month=month(d14$Pickup_date)   
d1$day=day(d1$Pickup_date) 
d14$day=day(d14$Pickup_date)  
d1$Pickup_date=strptime(d1$Pickup_date,format='%Y-%m-%d')   
d14$Pickup_date=strptime(d14$Pickup_date,format='%Y-%m-%d')   


#by month
ggplot(d1, aes(x=Pickup_date, y=Count, colour=factor(month))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_hour") +  
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab yearly volume by month')  

ggplot(d14, aes(x=Pickup_date, y=Count, colour=factor(month))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup_hour") +  
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab yearly volume by month')   


#by calender date 
d1$md <- as.Date(paste(d1$month, d1$day, sep="-"), "%m-%d") 
d1=d1[!d1$year=='2013',] 
d1$md=strptime(d1$md,format='%Y-%m-%d')   

 
ggplot(d1, aes(x=md, y=Count, colour=factor(year))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%m-%d")) +   
  geom_smooth(method = "loess",
              se = FALSE)+
  xlab("pickup day") +  
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab yearly volume by calender day')  

d14$md <- as.Date(paste(d14$month, d14$day, sep="-"), "%m-%d") 
d14=d14[!d14$year=='2013',] 
d14$md=strptime(d14$md,format='%Y-%m-%d')  

ggplot(d14, aes(x=md, y=Count, colour=factor(year))) + 
  geom_point()+  
  scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%m-%d")) +   
  geom_smooth(method = "loess",
              se = FALSE)+
  xlab("pickup day") +  
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab yearly volume by calender day')   




#visualize the delta ->d4, Difference in dropoff /pickup date for green cabs:
lll<-c("-1","2191","1","3","2","-3","0")
ggplot(d4, aes(x=factor(c(1:7)), y=Count)) + 
  geom_bar(stat="identity")+  
  xlab("delta (Difference in dropoff /pickup date)") +  
  ylab("count")+  
  scale_x_discrete(breaks=1:7,labels=lll)+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+ 
  ggtitle('green cab difference in dropoff /pickup date')   




#d20 green, d24 yellow 
d20$pickup_date=strptime(d20$pickup_date,format='%Y-%m-%d')   
d24$pickup_date=strptime(d24$pickup_date,format='%Y-%m-%d')   

#zooming in January 2015  
#green 
jan2015_g=d20[year(d20$pickup_date)==2015 & month(d20$pickup_date)==1,]
#yellow 
jan2015_y=d24[year(d24$pickup_date)==2015 & month(d24$pickup_date)==1,]

#zooming in January 2016
#green 
jan2016_g=d20[year(d20$pickup_date)==2016 & month(d20$pickup_date)==1,]
#yellow 
jan2016_y=d24[year(d24$pickup_date)==2016 & month(d24$pickup_date)==1,]

 

ggplot(jan2015_g, aes(x=pickup_date, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab ride count on jan_2015 , by pickup date and hour ')  
 
ggplot(jan2015_y, aes(x=pickup_date, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab ride count on jan_2015 , by pickup date and hour ')   
 
 

ggplot(jan2016_g, aes(x=pickup_date, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('green cab ride count on jan_2016 , by pickup date and hour ')  

ggplot(jan2016_y, aes(x=pickup_date, y=Count, colour=factor(pickup_hour))) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  #geom_smooth(method = "loess",
  #            se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('yellow cab ride count on jan_2016 , by pickup date and hour ')   






