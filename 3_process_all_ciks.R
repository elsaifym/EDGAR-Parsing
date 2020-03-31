###########################################
### 3_process_one_cik.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/26/20              ###
### Date modified: 03/30/20             ###
###########################################

###########################################################################################################
### This file executes all functions necessary to download, extract, and parse all 13F filings for a    ###
### given CIK. This is used along with a host of functions to batch the job using SLURM (see            ###
### submit_job.sh for batch job).                                                                       ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/3_process_one_cik.R', echo = TRUE)

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
library(XML)
library(parallel)

# source functions
source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_general.R')
source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_download.R')
source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_extract.R')
source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_parse.R')
source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_wrappers.R')

# set directory
setwd('/hpc/group/fuqua/mie4/edgar_parsing/data')

# load crsp
crspq <- fread('crspq.csv')
crspq[, yearqtr := as.yearqtr(yearqtr)]

# load cusip universe
load('cusip_universe/cusip6_universe.Rdata')

# get master folder, list all files
master_folder <- 'master_files/all_13f'
master_files <- list.files(master_folder)

# set locations to save the raw filings, the raw tables, the processed tables, and the biographical folder
raw_filings_folder <- 'raw_filings'
raw_tables_folder <- 'raw_tables'
processed_tables_folder <- 'processed_tables'
biographical_folder <- 'biographical'
error_file <- 'errors.csv'

# create all folders if they do not exist
if(!dir.exists(raw_filings_folder)) dir.create(raw_filings_folder)
if(!dir.exists(raw_tables_folder)) dir.create(raw_tables_folder)
if(!dir.exists(processed_tables_folder)) dir.create(processed_tables_folder)
if(!dir.exists(biographical_folder)) dir.create(biographical_folder)

# set overwrite flag (default to FALSE if value missing)
overwrite_download <- FALSE
overwrite_extract <- FALSE
overwrite_parse <- FALSE

##### STEP 1: GET THE ADDRESSES OF ALL FILINGS ASSOCIATED WITH ALL CIK #####

addresses <- get.all.addresses(folder = master_folder)

##########

##### STEP 2: DOWNLOAD ALL THOSE ADDRESSES #####

download.all.cik(addresses, output.folder = raw_filings_folder, overwrite = overwrite_download, sleep = 0)

##########

##### STEP 3: EXTRACT THE TABLES, GET BIOGRAPHICAL INFORMATION #####

# rbind addresses together
addresses_combined <- rbindlist(addresses)

# extract info
tablist <- extract.all.13f(addresses_combined, cusips = cusip6_universe, input.folder = raw_filings_folder, 
                           output.folder = raw_tables_folder, overwrite = overwrite_extract)

# get biographical information
biographical <- rbindlist(lapply(tablist, function(x) x$biographical))

# save entire biographical file
fwrite(biographical, paste(biographical_folder, 'biographical_full.csv', sep = '/'))

##########

##### STEP 4: PARSE THE TABLES #####

# parse the raw tables
tables <- parse.all.tables(addresses_combined, table.input.folder = raw_tables_folder, 
                           biographical.input.folder = biographical_folder, cusip.universe.all = cusip6_universe, 
                           crsp.universe.all = crspq, output.folder = processed_tables_folder, error.file = error_file, 
                           overwrite = overwrite_parse)

##########
