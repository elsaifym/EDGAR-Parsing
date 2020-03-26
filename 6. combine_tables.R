###########################################
### 6. combine_tables.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/19/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file combines the individual tables parsed in 5. parse_13f_data.R into a final database of 13F ###
### filings. Extreme observations and those with poor parsing results are dropped. Multiple CUSIPs      ###
### occurring in the same report are summed.                                                            ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/6. combine_tables.R', echo = TRUE)

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

# source functions
source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_general.R')

# set directory
setwd('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Data')

# load data
xmls <- readRDS('Raw Tables/xmls.rds')
fwfs <- readRDS('Raw Tables/fwfs.rds')
tabs <- readRDS('Raw Tables/tabs.rds')
csvs <- readRDS('Raw Tables/csvs.rds')
biographical <- fread('Final Tables/biographical_raw.csv')

##### MAKE MAIN RAW 13F DATA #####

# combine all into single tables into single data.table, dropping cusip9, remainder, comment, otherManager, all "num" columns
remove.cols <- function(dt, cols.to.remove) {
    # get column names to keep
    cols.to.keep <- colnames(dt)[!(grepl.multiple(cols.to.remove, colnames(dt), n = 1))]

    # only keep those cols
    dt <- dt[, ..cols.to.keep]

    # drop duplicated columns, return
    dt <- dt[, unique(colnames(dt)), with = FALSE]
    return(dt)
}
xml_combined <- rbindlist(lapply(xmls, 
                                 function(x) remove.cols(dt = x$table, cols.to.remove = c('remainder', 'cusip9', '^num', 
                                                                                          'otherManager', 'comment'))), 
                          fill = TRUE)
fwf_combined <- rbindlist(lapply(fwfs, 
                                 function(x) remove.cols(dt = x$table, cols.to.remove = c('remainder', 'cusip9', '^num', 
                                                                                          'otherManager', 'comment'))), 
                          fill = TRUE)
tab_combined <- rbindlist(lapply(tabs, 
                                 function(x) remove.cols(dt = x$table, cols.to.remove = c('remainder', 'cusip9', '^num', 
                                                                                          'otherManager', 'comment'))), 
                          fill = TRUE)
csv_combined <- rbindlist(lapply(csvs, 
                                 function(x) remove.cols(dt = x$table, cols.to.remove = c('remainder', 'cusip9', '^num', 
                                                                                          'otherManager', 'comment'))), 
                          fill = TRUE)

# add type to each
xml_combined[, type := 'xml'] 
fwf_combined[, type := 'fwf'] 
tab_combined[, type := 'tab'] 
csv_combined[, type := 'csv'] 

# rbind all together
holdings_raw <- rbindlist(list(xml_combined, fwf_combined, tab_combined, csv_combined), fill = TRUE)

# sort by cik, rdate, fdate, address, cusip8
holdings_raw <- holdings_raw[order(cik, rdate, fdate, address, cusip8), ]

##########

##### MAKE A CLEAN TABLE OF 13F DATA #####

# only keep certain cols from holdings_raw
cols.to.keep <- c('cik', 'rdate', 'fdate', 'form', 'cusip8', 'value', 'shares', 'shrsOrPrnAmt', 'putCall', 'prc', 'shrout', 
                  'split', 'deviation', 'in_universe', 'address', 'type')
holdings <- holdings_raw[, ..cols.to.keep]

# now, drop all call/put/prn obs, obs with missing price info, NA/0 shares, NA VALUE
holdings <- holdings[is.na(putCall) & shrsOrPrnAmt != 'PRN' & !is.na(prc) & !is.na(shares) & shares != 0, ]

# aggregate all holdings over cik, rdate, fdate, form, cusip8, address
holdings <- holdings[, list(shares = sum(na.omit(shares)), value = sum(na.omit(value)), prc = prc[1], 
                            shrout = shrout[1], split = split[1], type = type[1]), 
                     by = list(cik, rdate, fdate, form, cusip8, address)]

# re-compute deviation
holdings[, deviation := value / (prc * shares)]

# compute percent within 10% of deviation per report
holdings[, report_quality := max(sum(deviation >= 0.0009 & deviation <= 0.0011, na.rm = TRUE), 
                                 sum(deviation >= 0.9 & deviation <= 1.1, na.rm = TRUE), 
                                 sum(deviation >= 0.0000009 & deviation <= 0.0000011, na.rm = TRUE)) / sum(!is.na(prc)), 
         by = address]

# get quality as a standalone
quality <- unique(holdings[, c('address', 'type', 'report_quality')])

# get quality threshold (first percentile of xml filings)
quality_threshold <- quantile(quality[type == 'xml', ]$report_quality, 0.01, na.rm = TRUE)

# only keep holdings with reports above threshold (first percentile of XML filings, 48.71%)
holdings <- holdings[report_quality >= quality_threshold, ]

# only keep observations with rdates on months 3, 6, 9, 12
holdings <- holdings[substr(rdate, 5, 6) %in% c('03', '06', '09', '12'), ]

# make a within 10 percent variable
holdings[, within_10 := (deviation >= 0.0009 & deviation <= 0.0011) | 
                        (deviation >= 0.9 & deviation <= 1.1) | 
                        (deviation >= 0.0000009 & deviation <= 0.0000011)]

# compute percent ownership, only keep observations with ownership below the 99.95 percentile of XML filings (39.2%)
holdings[, ownership := shares / (shrout*1000)]
ownership_threshold <- quantile(holdings[type == 'xml', ]$ownership, 0.9995, na.rm = TRUE)
holdings <- holdings[ownership <= ownership_threshold, ]

# keep all observations with the maximum value of within_10 by cik, rdate, cusip8
holdings <- holdings[holdings[, .I[within_10 == max(within_10)], by = list(cik, rdate, cusip8)]$V1]

# finally, sort by cik, cusip8, rdate, fdate; take last value
holdings <- holdings[holdings[, .I[.N], by = list(cik, rdate, cusip8)]$V1]

# drop type, report_quality, within_10 cols
holdings <- holdings[, -c('type', 'report_quality', 'within_10')]

##########

##### FORMAT BIOGRAPHICAL #####

# merge quality to biographical
biographical <- merge(biographical, quality, by = 'address', all.x = TRUE)

# occassionally, a given filing (address) has multiple filing dates/ciks, etc. take the one that matches
biographical[, cik_match := cik == cik_row]
biographical <- unique(biographical[biographical[, .I[cik_match == max(cik_match)], by = address]$V1])

# drop cikname, cik, fdate_row, cik_match
biographical <- biographical[, -c('cikname', 'cik', 'fdate_row', 'cik_match')]

# make rdate, fdate dates
biographical[, `:=`(rdate = as.Date(as.character(rdate), format = '%Y%m%d'), 
                    fdate = as.Date(as.character(fdate), format = '%Y%m%d'))]

# format phone numbers
biographical[, phone := gsub(' |-|,|\\.|\\(|\\)|/|\\#|\\+|[[:alpha:]]', '', phone)]

# keep certain cols
biographical[, c('cik_row', 'cikname_row', 'rdate', 'fdate', 'form', 'has_table', 'type', 'report_quality', 
                 'street1', 'street2', 'city', 'state', 'zip', 'phone', 'address')]
colnames(biographical) <- c('cik', 'cikname', 'rdate', 'rdate', 'form', 'has_table', 'type', 'report_quality', 
                            'street1', 'street2', 'city', 'state', 'zip', 'phone', 'address')

##########

# save holdings, holdings_raw
fwrite(holdings, 'Final Tables/holdings.csv')
fwrite(holdings_raw, 'Final Tables/holdings_raw.csv')
fwrite(biographical, 'Final Tables/biographical.csv')
