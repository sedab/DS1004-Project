#Total amount & Tip data vs Taxi Volume Comparison in time domain
 
yellowcab <- read.csv2("yellow_cabs_daily_nicer.out",sep = "\t") #yellow data 
greencab <- read.csv2("green_cab_daily.out",sep = "\t")  
cost <- read.csv2("hypo2.out",sep = "\t")  
 
#convert the date
cost$Pickup.Date=strptime(cost$Pickup.Date,format='%m/%d/%Y')  
yellowcab$Pickup_date=strptime(yellow_cab$Pickup_date,format='%Y-%m-%d')  


for (i in 1:length(cost[,1])){  
  for(m in 1:length(yellowcab[,1])){
    if (cost$Pickup.Date[i]==yellowcab$Pickup_date[m]) {
      cost$yellowcab[i]=as.numeric(as.character(yellowcab$Count[m]))
    }}} 


cost$ratio=as.numeric(cost$Average.Tip.Amount)/as.numeric(cost$Average.Total.Amount)
result3<-c()
#correlations
result3[1]=cor(as.numeric(cost$Average.Tip.Amount), as.numeric(cost$yellowcab), method = c("pearson"))
result3[2]=cor(as.numeric(cost$Average.Total.Amount), as.numeric(cost$yellowcab), method = c("pearson"))
result3[3]=cor(as.numeric(cost$Average.Tip.Amount), as.numeric(cost$Average.Total.Amount), method = c("pearson"))
result3[4]=cor(as.numeric(cost$ratio), as.numeric(cost$yellowcab), method = c("pearson"))
