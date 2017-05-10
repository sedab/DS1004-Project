#Weather data vs Taxi Data Comparison in time domain
 

weather <- read.csv2("hypo1.out",sep = "\t") #weather data
yellow_cab <- read.csv2("yellow_cabs_daily_nicer.out",sep = "\t") #yellow data 
greencab <- read.csv2("green_cab_daily.out",sep = "\t")  

weather$Date=strptime(weather$Date,format='%m/%d/%Y')  
yellow_cab$Pickup_date=strptime(yellow_cab$Pickup_date,format='%Y-%m-%d')  
greencab$Pickup_date=strptime(greencab$Pickup_date,format='%Y-%m-%d')  

#combine the data sets 

for (i in 1:length(weather[,1])){  
  for(m in 1:length(greencab[,1])){
    if (weather$Date[i]==greencab$Pickup_date[m]) {
      weather$green_cab[i]=as.numeric(as.character(greencab$Count[m]))
    }}}  

for (i in 1:length(weather[,1])){  
  for(m in 1:length(yellow_cab[,1])){
    if (weather$Date[i]==yellow_cab$Pickup_date[m]) {
      weather$yellow_cab[i]=as.numeric(as.character(yellow_cab$Count[m]))
    }}} 

result<-c()
#correlations
result[1]=cor(as.numeric(weather$Temperature), as.numeric(weather$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(weather$Daily.Precipitation), as.numeric(weather$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(weather$Daily.Snow.Fall), as.numeric(weather$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(weather$Daily.Average.Wind.Speed), as.numeric(weather$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(weather$Daily.Peak.Wind.Speed), as.numeric(weather$green_cab), method = c("pearson"))
 
result[6]=cor(as.numeric(weather$Temperature), as.numeric(weather$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(weather$Daily.Precipitation), as.numeric(weather$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(weather$Daily.Snow.Fall), as.numeric(weather$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(weather$Daily.Average.Wind.Speed), as.numeric(weather$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(weather$Daily.Peak.Wind.Speed), as.numeric(weather$yellow_cab), method = c("pearson"))
 
write.csv(result,'weatherVsVolume.csv')




#zooming into certain dates 
#months 
m1=weather[month(weather$Date)==2 & year(weather$Date)==2014,] #feb 2014,  

result[1]=cor(as.numeric(m1$Temperature), as.numeric(m1$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(m1$Daily.Precipitation), as.numeric(m1$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(m1$Daily.Snow.Fall), as.numeric(m1$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(m1$Daily.Average.Wind.Speed), as.numeric(m1$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(m1$Daily.Peak.Wind.Speed), as.numeric(m1$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(m1$Temperature), as.numeric(m1$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(m1$Daily.Precipitation), as.numeric(m1$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(m1$Daily.Snow.Fall), as.numeric(m1$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(m1$Daily.Average.Wind.Speed), as.numeric(m1$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(m1$Daily.Peak.Wind.Speed), as.numeric(m1$yellow_cab), method = c("pearson"))
 


m2=weather[month(weather$Date)==1 & year(weather$Date)==2015,]#jan 2015,  

result[1]=cor(as.numeric(m2$Temperature), as.numeric(m2$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(m2$Daily.Precipitation), as.numeric(m2$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(m2$Daily.Snow.Fall), as.numeric(m2$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(m2$Daily.Average.Wind.Speed), as.numeric(m2$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(m2$Daily.Peak.Wind.Speed), as.numeric(m2$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(m2$Temperature), as.numeric(m2$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(m2$Daily.Precipitation), as.numeric(m2$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(m2$Daily.Snow.Fall), as.numeric(m2$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(m2$Daily.Average.Wind.Speed), as.numeric(m2$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(m2$Daily.Peak.Wind.Speed), as.numeric(m2$yellow_cab), method = c("pearson"))
 


m3=weather[month(weather$Date)==1 & year(weather$Date)==2016,]#jan 2016  

result[1]=cor(as.numeric(m3$Temperature), as.numeric(m3$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(m3$Daily.Precipitation), as.numeric(m3$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(m3$Daily.Snow.Fall), as.numeric(m3$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(m3$Daily.Average.Wind.Speed), as.numeric(m3$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(m3$Daily.Peak.Wind.Speed), as.numeric(m3$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(m3$Temperature), as.numeric(m3$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(m3$Daily.Precipitation), as.numeric(m3$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(m3$Daily.Snow.Fall), as.numeric(m3$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(m3$Daily.Average.Wind.Speed), as.numeric(m3$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(m3$Daily.Peak.Wind.Speed), as.numeric(m3$yellow_cab), method = c("pearson"))


#weeks  
for(d in 20:27){
w1[d-19,]=weather[month(weather$Date)==2 & year(weather$Date)==2014 & (day(weather$Date)==d),] #feb 20-27 2014  
w2[d-19,]=weather[month(weather$Date)==1 & year(weather$Date)==2015 & (day(weather$Date)==d),]#Jan 20-27 2015
w3[d-19,]=weather[month(weather$Date)==1 & year(weather$Date)==2016 & (day(weather$Date)==d),]#jan 20-27 2016
}


#for w1
result[1]=cor(as.numeric(w1$Temperature), as.numeric(w1$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(w1$Daily.Precipitation), as.numeric(w1$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(w1$Daily.Snow.Fall), as.numeric(w1$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(w1$Daily.Average.Wind.Speed), as.numeric(w1$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(w1$Daily.Peak.Wind.Speed), as.numeric(w1$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(w1$Temperature), as.numeric(w1$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(w1$Daily.Precipitation), as.numeric(w1$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(w1$Daily.Snow.Fall), as.numeric(w1$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(w1$Daily.Average.Wind.Speed), as.numeric(w1$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(w1$Daily.Peak.Wind.Speed), as.numeric(w1$yellow_cab), method = c("pearson"))


#for w2
result[1]=cor(as.numeric(w2$Temperature), as.numeric(w2$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(w2$Daily.Precipitation), as.numeric(w2$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(w2$Daily.Snow.Fall), as.numeric(w2$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(w2$Daily.Average.Wind.Speed), as.numeric(w2$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(w2$Daily.Peak.Wind.Speed), as.numeric(w2$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(w2$Temperature), as.numeric(w2$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(w2$Daily.Precipitation), as.numeric(w2$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(w2$Daily.Snow.Fall), as.numeric(w2$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(w2$Daily.Average.Wind.Speed), as.numeric(w2$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(w2$Daily.Peak.Wind.Speed), as.numeric(w2$yellow_cab), method = c("pearson"))


#for w3
result[1]=cor(as.numeric(w3$Temperature), as.numeric(w3$green_cab), method = c("pearson"))
result[2]=cor(as.numeric(w3$Daily.Precipitation), as.numeric(w3$green_cab), method = c("pearson"))
result[3]=cor(as.numeric(w3$Daily.Snow.Fall), as.numeric(w3$green_cab), method = c("pearson"))
result[4]=cor(as.numeric(w3$Daily.Average.Wind.Speed), as.numeric(w3$green_cab), method = c("pearson"))
result[5]=cor(as.numeric(w3$Daily.Peak.Wind.Speed), as.numeric(w3$green_cab), method = c("pearson"))

result[6]=cor(as.numeric(w3$Temperature), as.numeric(w3$yellow_cab), method = c("pearson"))
result[7]=cor(as.numeric(w3$Daily.Precipitation), as.numeric(w3$yellow_cab), method = c("pearson"))
result[8]=cor(as.numeric(w3$Daily.Snow.Fall), as.numeric(w3$yellow_cab), method = c("pearson"))
result[9]=cor(as.numeric(w3$Daily.Average.Wind.Speed), as.numeric(w3$yellow_cab), method = c("pearson"))
result[10]=cor(as.numeric(w3$Daily.Peak.Wind.Speed), as.numeric(w3$yellow_cab), method = c("pearson"))

