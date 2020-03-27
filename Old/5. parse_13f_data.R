###########################################
### 5. parse_13f_data.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/15/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file parses the 13F tables extracted from 4. extract_13f_data.R into a set of data tables. For ###
### post-2013 filings, all information is in XML format and is thus easy to extract. Prior to 2013, the ###
### filings have very different formatting. For these filings, this file extracts the CUSIP, whether    ###
### the position is reported as shares or principal amount, whether it is a put or a call, the          ###
### investment discretion of the manager, and the "shares" and "value" column. CRSP data is used to     ###
### validate which column is shares and which is value by determining which makes more sense given      ###
### prices in CRSP.                                                                                     ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/5. parse_13f_data.R', echo = TRUE)

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
library(parallel)
library(XML)

# source functions
source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_general.R')
source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_parse.R')

# set directory
setwd('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Data')

# load crsp
crspq <- fread('crspq.csv')
crspq[, yearqtr := as.yearqtr(yearqtr)]

# load cusip universe
load('CUSIP Universe/cusip6_universe.Rdata')

# load data
dir <- 'Table Rows'
to.read <- list.files(dir)[order(as.numeric(gsub('tables_|.rds', '', list.files(dir))))]
load.one.tablist <- function(file.name, dir = dir, counter, total, start.time) {
    # load one file
    temp <- readRDS(paste(dir, file.name, sep = '/'))

    # print progress
    progress(counter, total, start.time, 'Loaded')

    # return temp
    return(temp)
}
start <- Sys.time()
tablists <- mclapply(1:length(to.read), function(i) load.one.tablist(to.read[i], dir = dir, counter = i, 
                                                                     total = length(to.read), start.time = start), 
                     mc.cores = detectCores())

# combine into a single list
tablist <- unlist(tablists, recursive = FALSE)

##### GET BIOGRAPHICAL INFORMATION #####

biographical <- rbindlist(mclapply(tablist, function(x) cbind(x$biographical, has_table = x$tableinfo$type != 'none')))
fwrite(biographical, 'Final Tables/biographical_raw.csv')

##########

##### SPLIT TABLIST BY TABLETYPE #####

# summarize types of files
table(unlist(lapply(tablist, function(x) x$tableinfo$type)))

# get different types separately
csvs <- tablist[sapply(tablist, function(x) x$tableinfo$type == 'csv')]
fwfs <- tablist[sapply(tablist, function(x) x$tableinfo$type == 'fwf')]
nones <- tablist[sapply(tablist, function(x) x$tableinfo$type == 'none')]
tabs <- tablist[sapply(tablist, function(x) x$tableinfo$type == 'tab')]
xmls <- tablist[sapply(tablist, function(x) x$tableinfo$type == 'xml')]

##########

##### PARSE XML TABLES #####

# parse all xml files (split into groups of 1000 to prevent overloading memory)
xml_tables <- wrapper.split(xmls, 1000, parse.all.tables, cusip.universe.all = cusip6_universe, 
                            crsp.universe.all = crspq, num.cores = 20)

n <- 1000
xml_tables <- as.list(rep(NA, length(xmls)))
for(i in 1:ceiling(length(xmls)/n)) {
    # get tables for group i
    temp <- parse.all.tables(xmls[(1 + (i-1)*n):min((i*n), length(xmls))], 
                             cusip.universe.all = cusip6_universe, crsp.universe.all = crspq, 
                             num.cores = 20)

    # append to xml_tables
    xml_tables[(1 + (i-1)*n):min((i*n), length(xmls))] <- temp

    # print progress
    cat(paste('Done', i, 'of', ceiling(length(xmls)/n), 'sets!', '\n'))
}

# summarize accuracy 
xml_summary <- lapply(xml_tables, function(x) max(sum(x$table$deviation >= 0.0009 & x$table$deviation <= 0.0011, na.rm = TRUE),
                                                  sum(x$table$deviation >= 0.9 & x$table$deviation <= 1.1, na.rm = TRUE))  / 
                                                    sum(!is.na(x$table$prc)))
summary(unlist(xml_summary))

# save
saveRDS(xml_tables, file = 'Raw Tables/xmls.rds')

##########

##### PARSE FWF TABLES #####

# parse all fwf files
fwf_tables <- parse.all.tables(fwfs, cusip.universe.all = cusip6_universe, crsp.universe.all = crspq)

# summarize accuracy 
fwf_summary <- lapply(fwf_tables, function(x) max(sum(x$table$deviation >= 0.0009 & x$table$deviation <= 0.0011, na.rm = TRUE),
                                                  sum(x$table$deviation >= 0.9 & x$table$deviation <= 1.1, na.rm = TRUE))  / 
                                                    sum(!is.na(x$table$prc)))
summary(unlist(fwf_summary))

# save
saveRDS(fwf_tables, file = 'Raw Tables/fwfs.rds')

##########

##### PARSE TAB TABLES #####

# parse all tab files
tab_tables <- parse.all.tables(tabs, cusip.universe.all = cusip6_universe, crsp.universe.all = crspq)

# summarize accuracy 
tab_summary <- lapply(tab_tables, function(x) max(sum(x$table$deviation >= 0.0009 & x$table$deviation <= 0.0011, na.rm = TRUE),
                                                  sum(x$table$deviation >= 0.9 & x$table$deviation <= 1.1, na.rm = TRUE))  / 
                                                    sum(!is.na(x$table$prc)))
summary(unlist(tab_summary))

# save
saveRDS(tab_tables, file = 'Raw Tables/tabs.rds')

##########

##### PARSE CSV TABLES #####

# first, fix some of the csv labelling (some labelled as csv are actually misc. tables)
types <- c()
for(i in 1:length(csvs)) {
    progress_prompt <- paste('Done', i, 'of', length(csvs), 'tables! \n')
    question_prompt <- 'What is the tabletype (enter fwf, tab, csv, none, or other): '
    print(csvs[[i]]$tableinfo$table)
    type <- 'NULL'
    while(!type %in% c('fwf', 'tab', 'csv', 'none', 'other')) {
        type <- readline(cat(progress_prompt, question_prompt))
    }
    types <- c(types, type)
}

# apply types to csv
for(i in 1:length(csvs)) {
    csvs[[i]]$tableinfo$type <- types[i]
}

# parse all csv files
csv_tables <- parse.all.tables(csvs, cusip.universe.all = cusip6_universe, crsp.universe.all = crspq, num.cores = 20)

# summarize accuracy 
csv_summary <- lapply(csv_tables, function(x) max(sum(x$table$deviation >= 0.0009 & x$table$deviation <= 0.0011, na.rm = TRUE),
                                                  sum(x$table$deviation >= 0.9 & x$table$deviation <= 1.1, na.rm = TRUE))  / 
                                                    sum(!is.na(x$table$prc)))
summary(unlist(csv_summary))

# save
saveRDS(csv_tables, file = 'Raw Tables/csvs.rds')

##########
