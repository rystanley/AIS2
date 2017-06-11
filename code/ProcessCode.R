#Load libraries
library(data.table)
library(dplyr)
library(reshape2)
library(lubridate)
library(readr)
library(feather)
library(maps)
library(mapdata)

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


## Create ship specific datafiles ##############

  #Find the positional data
    datafiles <- paste0("data/",files[grep("Msg1",files)])
  
  #create folder to store ship specific files
    if(length(which(list.files(getwd())=="MMSI_files"))==0){dir.create(paste0(getwd(),"/MMSI_files"))} 
  
  #Warning message to make sure you don't make super large files by accident. 
    if(!length(which(list.files(getwd())=="MMSI_files"))==0 & length(dir("MMSI_files/"))>0){
          print(paste0("Before running the next loop make sure all files contained in ",
                       getwd(),"/MMSI_files are supposed to be there, else they will have extra data added to them."))} 


##Loop through and create files.
    Start <- Sys.time() # Start timer

        for(ind in datafiles){
          
          #read in the data
            temp <- fread(ind,header=T, stringsAsFactors=FALSE)
          
          #Data cleaning
            temp <- as.data.frame(temp[temp$MsgType==1,]) # data.table creates a weird format. Conver to dataframe for easier handling.
            cols <- as.numeric(setdiff(1:length(temp),grep("SourceFile|RecCnt",names(temp)))) #get rid of the stuff
            temp <- temp[,cols] # get rid of the columns we don't need. 
            
          #Within this data file which are the unqiue ships
            MMSIunique <- unique(temp$MMSI)
          
          #Loop through and write the files. If the file exists data will be appended for that ship.
            for (i in MMSIunique){
              dplyr::filter(temp,MMSI==i)%>%fwrite(.,file = paste0("MMSI_files/",i,".txt"),append=T)
            } #END 'i' LOOP
            
        } #END 'ind' LOOP

      Sys.time()-Start #end timer


## Truncate the files at a half our interval ################

    #Clean up the ship files and fix date-time
      shipfiles <- dir("MMSI_files/")
    
    #create folder to put truncated files
      if(length(which(list.files(getwd())=="Trunc_files"))==0){dir.create(paste0(getwd(),"/Trunc_files"))} 
    
    Start <- Sys.time() #time stamp
    
      for (ship in shipfiles){
        cnt <- grep(ship,shipfiles)
        
        print(paste0("File ",cnt," of ",length(shipfiles),". ",round(cnt/length(shipfiles),2)*100,"% complete"))
        
        temp <- fread(paste0("MMSI_files/",ship),header=T,stringsAsFactors = F)
        temp$DateTime <- lubridate::ymd_hms(temp$DateTime)
      
        temp <- as.data.frame(temp) # convert to dataframe
        temp <- temp[order(temp$DateTime),] #order by time
        temp$cut <- cut(temp$DateTime, breaks = "30 mins",labels=F) #cluster into 30 minute frames
        
        temp <- temp%>%group_by(cut)%>%filter(row_number()==1)%>%ungroup()%>%data.frame() #Grab the first observation for each half hour segment
        
        fwrite(x=temp,file = paste0("Trunc_files/Ship_",ship))
        
      } # end of ship truncate loop
      
    Sys.time()-Start # how long did it take.

## Map out the truncated results ###############
    
    ## Set map limits 
      Lat.lim=c(38,52)
      Long.lim=c(-75,-48)
    ## Set plotting domain and axes
      map("worldHires", xlim=Long.lim, ylim=Lat.lim, col="black", fill=FALSE, resolution=0) 
      map.axes();map.scale(ratio=FALSE)

      tfiles <- dir("Trunc_files/") #files created by previous truncated step

      #Loop through and add lines to the plot
        for (i in tfiles){
          temp <- as.data.frame(fread(paste0("Trunc_files/",i),header=T,stringsAsFactors = F))
          lines(temp$Longt,temp$Lat,type="l",lwd=0.01)
        }

      #Add the map land over the tracks. Some go over land
        map("worldHires", xlim=Long.lim, ylim=Lat.lim, col="white", fill=TRUE, resolution=0,add=T)


## Merge in the metadata #############
        
shipfiles <- dir("Trunc_files/")

## Merge each file with MMSI
      for (ind in datafiles){
          temp <- temp <- fread(paste0("MMSI_files/",ind),header=T, stringsAsFactors=FALSE)
          temp <- merge(temp,mdata,by="MMSI")
       } #end of loop

