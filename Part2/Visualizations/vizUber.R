#uber vizualzization 

uber2014 <- read.csv("uber_2014_daily.out",sep = ",") # 
uber2015 <- read.csv("uber_2015_daily.out",sep = ",") # 

txt <- gsub("[()]", "", readLines("uber_2014_daily.out"))
uber2014 <-read.csv(text = txt, header = FALSE) 
txt2 <- gsub("[()]", "", readLines("uber_2015_daily.out"))
uber2015 <-read.csv(text= txt2, header = FALSE, sep="")  

uber2014$V1 <- gsub("'", '',uber2014$V1)
uber2015_=uber2015[2:182,]

uber2014$V1=strptime(uber2014$V1,format='%m/%d/%Y')  
uber2015_$V1=strptime(uber2015_$V1,format='%Y-%m-%d') 


yellowcab <- read.csv2("yellow_cabs_daily_nicer.out",sep = "\t") #yellow data 
greencab <- read.csv2("green_cab_daily.out",sep = "\t")  

yellowcab$Pickup_date=strptime(yellowcab$Pickup_date,format='%Y-%m-%d')  
greencab$Pickup_date=strptime(greencab$Pickup_date,format='%Y-%m-%d') 

#filter the yellow and green cab years 
greencab2014=greencab[year(greencab$Pickup_date)==2014, ]
greencab2015=greencab[year(greencab$Pickup_date)==2015, ]

yellowcab2014=yellowcab[year(yellowcab$Pickup_date)==2014, ]
yellowcab2015=yellowcab[year(yellowcab$Pickup_date)==2015, ]



#combine the data sets 
for (i in 1:length(uber2014[,1])){  
  for(m in 1:length(greencab2014[,1])){
    if (uber2014$V1[i]==greencab2014$Pickup_date[m]) {
      uber2014$green_cab[i]=as.numeric(as.character(greencab2014$Count[m]))
    }}}

for (i in 1:length(uber2014[,1])){  
  for(m in 1:length(yellowcab2014[,1])){
    if (uber2014$V1[i]==yellowcab2014$Pickup_date[m]) {
      uber2014$yellow_cab[i]=as.numeric(as.character(yellowcab2014$Count[m]))
    }}}




for (i in 1:length(uber2015_[,1])){  
  for(m in 1:length(greencab2015[,1])){
    if (uber2015_$V1[i]==greencab2015$Pickup_date[m]) {
      uber2015_$green_cab[i]=as.numeric(as.character(greencab2015$Count[m]))
    }}}

for (i in 1:length(uber2015_[,1])){  
  for(m in 1:length(yellowcab2015[,1])){
    if (uber2015_$V1[i]==yellowcab2015$Pickup_date[m]) {
      uber2015_$yellow_cab[i]=as.numeric(as.character(yellowcab2015$Count[m]))
    }}}  

 
#melt 2 files 
colnames(uber2014)[colnames(uber2014) == 'V2'] <- 'uber'
colnames(uber2015_)[colnames(uber2015_) == 'V2'] <- 'uber'

all2014<- melt(uber2014, id=c("V1"))  
all2015<- melt(uber2015_, id=c("V1")) 

all2014$V1=strptime(all2014$V1,format='%Y-%m-%d') 
all2015$V1=strptime(all2015$V1,format='%Y-%m-%d') 

 
ggplot(all2014, aes(x=V1, y=value, colour=variable)) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  geom_smooth(method = "loess", se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('uber compared to yellow and green cab volume in 2014')   


ggplot(all2015, aes(x=V1, y=as.numeric(value), colour=variable)) + 
  geom_line()+  
  #scale_x_datetime(breaks=date_breaks("1 months"), labels=date_format("%Y-%m-%d")) +  
  xlab("pickup date") +  
  geom_smooth(method = "loess", se = FALSE)+
  ylab("count")+
  #scale_y_continuous(limit = c(0, 1200))+
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+ 
  theme(axis.text.y = element_blank())+
  ggtitle('uber compared to yellow and green cab volume in 2015')   


