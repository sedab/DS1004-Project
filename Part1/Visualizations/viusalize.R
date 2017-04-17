#this script is to visualize the TLC data for DSGA1004 Project 
#date:04/17/17  

q1_g <- read.csv2("query1_g.out",sep = "\t") 
q1_y <- read.csv2("query1_y.out",sep = "\t") 
q2_g <- read.csv2("query2_g.out",sep = "\t") 
q2_y <- read.csv2("query2_y.out",sep = "\t") 

#install the ggplot2 package:
install.packages("ggplot2")
#load it
library("ggplot2")

#plot Green Cab Counts for Vendor Id vs Payemnt type   
ggplot(q1_g, aes(x=Vendor.ID, y=Count, fill=Payment.Type)) +  
        geom_bar(position=position_dodge(), stat="identity")+
        xlab("vendor id") +
        ylab("taxi count")+ 
        ggtitle("Green Cab Counts for Vendor Id vs Payemnt type ")

#plot Yellow Cab Counts for Vendor Id vs Payemnt type 
ggplot(q1_y, aes(x=Vendor.ID, y=Count, fill=Payment.Type)) +  
  geom_bar(position=position_dodge(), stat="identity")+
  xlab("vendor id") +
  ylab("taxi count")+ 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Yellow Cab Counts for Vendor Id vs Payemnt type ")


#plot Yellow Cab hours of the day vs percentage per rate code 
ggplot(q2_y, aes(x=Time.of.Day, y=Percentage, group=Rate.Code, colour=Rate.Code)) +  
  geom_line()+##position=position_dodge(), stat="identity")+
  xlab("hour of the day") +
  ylab("percentage")+  
  theme(axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank())+
  ggtitle("Yellow Cab percentage for the hours of the day ") 
 
#plot Green Cab hours of the day vs percentage per rate code 
ggplot(q2_g, aes(x=Time.of.Day, y=Percentage, group=Rate.Code, colour=Rate.Code)) +  
  geom_line()+##position=position_dodge(), stat="identity")+
  xlab("hour of the day") +
  ylab("percentage")+  
  theme(axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank())+
  ggtitle("Green Cab percentage for the hours of the day ")