###########################################
### 3_process_one_cik.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/26/20              ###
### Date modified: 03/26/20             ###
###########################################

###########################################################################################################
### This file executes all functions necessary to download, extract, and parse all 13F filings for a    ###
### given CIK. This is used along with functions_parallel.R to batch the job.                           ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/EDGAR Parsing/Code/3. process_one_cik.R', echo = TRUE)

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

# source functions
source('/hpc/group/fuqua/mie4/EDGAR Parsing/Code/Functions/functions_parallel.R')

# set directory
setwd('/hpc/group/fuqua/mie4/EDGAR Parsing/Data')

# load crsp
crspq <- fread('crspq.csv')
crspq[, yearqtr := as.yearqtr(yearqtr)]

# load cusip universe
load('CUSIP Universe/cusip6_universe.Rdata')

# get slurm value
ind <- as.integer(Sys.getenv('SLURM_ARRAY_TASK_ID'))

# get master folder, list all files
master_folder <- 'Master Files/All_13F'
master_files <- list.files(master_folder)

# set locations to save the raw filings, the raw tables, the processed tables, and the biographical folder
raw_filings_folder <- 'Raw Filings'
raw_tables_folder <- 'Raw Tables'
processed_tables_folder <- 'Processed Tables'
biographical_folder <- 'Biographical Tables'

# create all folders if they do not exist
if(!dir.exists(raw_filings_folder)) dir.create(raw_filings_folder)
if(!dir.exists(raw_tables_folder)) dir.create(raw_tables_folder)
if(!dir.exists(processed_tables_folder)) dir.create(processed_tables_folder)
if(!dir.exists(biographical_folder)) dir.create(biographical_folder)

##### STEP 1: GET THE ADDRESSES OF ALL FILINGS ASSOCIATED WITH THAT CIK #####

addresses <- get.one.address(master_files[ind], folder = master_folder)

##########

##### STEP 2: DOWNLOAD ALL THOSE ADDRESSES #####

download.one.cik(addresses, output.folder = raw_filings_folder, overwrite = FALSE)

##########

##### STEP 3: EXTRACT THE TABLES, GET BIOGRAPHICAL INFORMATION #####

# extract info
tablist <- lapply(1:nrow(addresses), function(i) extract.one.13f(row = addresses[i, ], input.folder = raw_filings_folder,
                                                                 output.folder = raw_tables_folder, overwrite = TRUE))

# get biographical information
biographical <- rbindlist(lapply(tablist, function(x) cbind(x$biographical, has_table = length(x$table) > 0)))

# save biographical for that cik
fwrite(biographical, paste(biographical_folder, paste0(unique(addresses$cik), '.csv'), sep = '/'))

##########

##### STEP 4: PARSE THE TABLES #####

# first, remove all entries in tablist with NULL tables
tablist <- tablist[unlist(lapply(tablist, function(x) !is.null(x[[2]])))]

# determine the types of all tables, drop all "none" types
tablist <- tablist[unlist(lapply(lapply(tablist, function(x) x$table), determine.type)) != 'none']

# get tables and save
tables <- lapply(tablist, parse.one.table, cusip.universe.all = cusip6_universe, crsp.universe.all = crspq, 
                 output.folder = processed_tables_folder, overwrite = TRUE)

##########