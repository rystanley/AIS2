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


#positoinal datafiles
datafiles <- paste0("data/",files[grep("Msg1",files)])

#create folder to dump files
if(length(which(list.files(getwd())=="MMSI_files"))==0){dir.create(paste0(getwd(),"/MMSI_files"))} 

#Warning message to make sure you don't make super large files by accident. 
if(!length(which(list.files(getwd())=="MMSI_files"))==0 & length(dir("MMSI_files/"))>0){
      print(paste0("Before running the next loop make sure all files contained in ",
                   getwd(),"/MMSI_files are supposed to be there, else they will have extra data added to them."))} 


##Loop through and create files.

Start <- Sys.time() # Start timer

for(ind in datafiles){
  
  temp <- fread(ind,header=T, stringsAsFactors=FALSE)
  
  #Data cleaning
  temp <- as.data.frame(temp[temp$MsgType==1,])
  cols <- as.numeric(setdiff(1:length(temp),grep("SourceFile|RecCnt",names(temp))))
  temp <- temp[,cols] # get rid of the columns we don't need. 
  
  MMSIunique <- unique(temp$MMSI)
  
  for (i in MMSIunique){
    dplyr::filter(temp,MMSI==i)%>%fwrite(.,file = paste0("MMSI_files/",i,".txt"),append=T)
  }
  
}

Start-Sys.time() #end timer


shipfiles <- dir("MMSI_files/")
shipfiles <- as.numeric(gsub(".txt","",shipfiles))

## Merge each file with MMSI
for (ind in datafiles){
   
   temp <- temp <- fread(paste0("MMSI_files/",ind),header=T, stringsAsFactors=FALSE)
   temp <- merge(temp,mdata,by="MMSI")
   
   
#   
#   
#   
# }

