###########################################
### 1_get_master_files.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/27/20              ###
### Date modified: 03/27/20             ###
###########################################

###########################################################################################################
### This file downloads the master files from EDGAR. Master files contain a list of all filings by SEC  ###
### reporting institutions and list details of filing CIK, name, form type, and address.                ###
###########################################################################################################

# source('/hps/group/fuqua/mie4/data_projects/edgar_parsing/code/1_get_master_files.R', echo = TRUE)

# clear
rm(list = ls())

# misc
options(width = 135) # set console width
options(menu.graphics = FALSE) # disable menu popups in XQuartz

# set seed
set.seed(05041995)

# load packages
library(data.table)
library(zoo)

# set directory
setwd('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/data')

# set output folder
output_folder <- 'master_files'
if(!dir.exists(output_folder)) dir.create(output_folder)

# set start, end dates for master files
start_year <- 1993
end_year <- year(Sys.Date())
end_quarter <- ceiling(month(Sys.Date())/3)

##### DOWNLOAD THE MASTER FILES #####

# iterate over years
for(year in start_year:end_year) {
    # iterate over quarter
    for(quarter in paste0('QTR', 1:4)) {
        # get file address
        address <- paste('https://www.sec.gov/Archives/edgar/full-index', year, quarter, 'master.idx', sep = '/')

        # download the file
        download.file(address, paste(output_folder, paste0(year, quarter, 'master.txt'), sep = '/'))

        # if in end year and exceed end_quarter, break
        if(year == end_year & as.numeric(gsub('QTR', '', quarter)) >= end_quarter) break 
    }
}

##########
