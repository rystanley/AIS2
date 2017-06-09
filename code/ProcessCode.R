#Load libraries
library(data.table)
library(dplyr)
library(reshape2)
library(lubridate)

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


for(ind in datafiles){
  
  temp <- fread(ind,header=T, stringsAsFactors=FALSE)
  
  #Data cleaning
  temp <- temp[temp$MsgType==1,]
  temp <- temp[,-grep("SourceFile|RecCnt",names(temp))]
  
  
  
  
}



datafilter <- function(ind){
  
  
  Vdata<-fread(paste0("data/TAIS_Msg1File_",ind,".txt"), header = TRUE, stringsAsFactors=FALSE)
  hdata <- as.data.frame(fread(paste0("data/TAIS_Msg5File_",ind,".txt"),header = TRUE,stringsAsFactors = FALSE))
  
  expanddata <- merge(Vdata)
  
  
} #end of datafilter function