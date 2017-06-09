#Load libraries
library(data.table)
library(dplyr)
library(reshape2)
library(lubridate)
library(readr)
library(feather)

#get list of data files
files <- dir("data/")
temp <- as.data.frame(do.call(rbind, strsplit(files,"_"))) #break up data files for function
temp[,3] <- gsub(".txt","",temp[,3]) #get rid of '.txt' at the end of the file call 'index.txt' - to 'index'

#this is a vector which identifies the files that are unique to eachother
uniqueData <- unique(temp[,3])


## Read in ship metadata files
mdata_files <- paste0("data/",files[grep("Msg5",files)])

tables <- lapply(mdata_files,fread,header=T, stringsAsFactors=FALSE)
mdata <- as.data.frame(do.call(rbind , tables))
mdata <- subset(mdata,!duplicated(mdata$MMSI)) # get rid of any duplicateed ship entries
mdata <- mdata[,c("MMSI","IMONumber","ShipType","ShipLength","ShipBeam","Draft")] #trim data not required


datafiles <- paste0("data/",files[grep("Msg1",files)])

##Loop through and create files.

for(ind in datafiles){
  
  temp <- fread(ind,header=T, stringsAsFactors=FALSE)
  
  #Data cleaning
  temp <- as.data.frame(temp[temp$MsgType==1,])
  cols <- as.numeric(setdiff(1:length(temp),grep("SourceFile|RecCnt",names(temp))))
  temp <- temp[,cols] # get rid of the columns we don't need. 
  
  temp%>%group_by(MMSI)%>%data.frame()%>%fwrite()
  
  MMSIunique <- unique(temp$MMSI)
  
  #create folder to dump files
  if(length(which(list.files(getwd())=="MMSI files"))==0){dir.create(paste0(getwd(),"/MMSI_files"))} 
  
  for (i in MMSIunique){
    dplyr::filter(temp,MMSI==i)%>%fwrite(.,file = paste0("MMSI_files/",i,".txt"),append=T)
  }
  
}

## Merge each file with MMSI
# for (ind in datafiles){
#   
#   temp <- temp <- fread(ind,header=T, stringsAsFactors=FALSE)
#   temp <- merge(temp,mdata,by="MMSI")
#   
#   #S
#   
#   
#   
# }

