#gas price vs total amount & tip amount 

gas <- read.csv2("Monthly Regular Grade Motor Gasoline Prices.csv",sep = "\t")  
cost <- read.csv2("hypo2.out",sep = "\t")  

cost$Pickup.Date=strptime(cost$Pickup.Date,format='%m/%d/%Y')  


# d18 gasoline price
gas$date <-paste(gas$month, '1', gas$year,  sep = "/")
gas$da=strptime(gas$date,format='%m/%d/%Y')   



for (i in 1:length(cost[,1])){  
  for(m in 1:length(gas[,1])){
    if (month(cost$Pickup.Date[i])==month(gas$da[m]) & year(cost$Pickup.Date[i])==year(gas$da[m])){
      cost$gas_price[i]=(as.numeric(as.character(gas$price[m])))
    }}}  

result4<-c()
result4[1]=cor(as.numeric(cost$Average.Tip.Amount), as.numeric(cost$gas_price), method = c("pearson"))
result4[2]=cor(as.numeric(cost$Average.Total.Amount), as.numeric(cost$gas_price), method = c("pearson"))


