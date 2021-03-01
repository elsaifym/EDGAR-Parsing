###########################################
### 4_combine_tables.R                  ###
### Author: Morad Elsaify               ###
### Date created: 03/28/20              ###
### Date modified: 02/27/21             ###
###########################################

###########################################################################################################
### This file combines the individual tables parsed in 3_process_one_cik.R or 3_process_all_ciks.R into ###
### a final database of 13F filings and a set of biographical information about filers. Extreme         ###
### observations and those with poor parsing results are dropped. Multiple CUSIPs occurring in the same ###
### report are aggregated.                                                                              ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/4_combine_tables.R', echo = TRUE)

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

# source general functions
source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/functions/functions_general.R')
source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/functions/functions_wrappers.R')

# set directory
setwd('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/data')

# get all tables
table_folder <- 'processed_tables'
table_files <- list.files(table_folder, recursive = TRUE)
table_files <- paste(table_folder, table_files, sep = '/')
tables_load <- wrapper.split(table_files, 10000, load.all.files, colClasses = 'character', num.cores = detectCores())
tables <- lapply(tables_load, function(x) x$value)

# get all biographical information (not including full file)
biographical_folder <- 'biographical'
bio_files <- list.files(biographical_folder)
bio_files <- paste(biographical_folder, bio_files[!grepl('biographical_full', bio_files)], sep = '/')
bios_load <- load.all.files(bio_files, colClasses = 'character')
bios <- lapply(bios_load, function(x) x$value)

##### MAKE A RAW TABLE OF 13F DATA #####

# combine all into single tables into single data.table, dropping cusip9, remainder, comment, otherManager, all "num" columns
remove_raw <- c('remainder', 'cusip9', '^num', 'otherManager', 'comment')
remove.cols <- function(dt, cols.to.remove) {
    # get column names to keep
    cols.to.keep <- colnames(dt)[!(grepl.multiple(cols.to.remove, colnames(dt), n = 1))]

    # only keep those cols
    dt <- dt[, ..cols.to.keep]

    # drop duplicated columns, return
    dt <- dt[, unique(colnames(dt)), with = FALSE]
    return(dt)
}
tables <- lapply(tables, remove.cols, cols.to.remove = remove_raw)

# rbind all together, make raw holdings table
holdings_raw <- rbindlist(tables, fill = TRUE)

# convert variables to numeric
holdings_raw[, `:=`(value = as.numeric(value), shares = as.numeric(shares), prc = as.numeric(prc), 
                    shrout = as.numeric(shrout), deviation = as.numeric(deviation))]

# save holdings_raw
fwrite(holdings_raw, 'final_tables/holdings_raw.csv')

##########

##### MAKE A CLEAN TABLE OF 13F DATA #####

# remove cols, remove holdings_raw to save memory
holdings <- holdings_raw[, -c('investmentDiscretion', 'nameOfIssuer', 'titleOfClass', 'votingAuthority')]
rm(holdings_raw)

# convert variables to numeric
holdings[, `:=`(value = as.numeric(value), shares = as.numeric(shares), prc = as.numeric(prc), 
                shrout = as.numeric(shrout), deviation = as.numeric(deviation))]

# now, drop all call/put/prn obs, obs with missing price info, NA/0 shares, NA value
holdings <- holdings[(is.na(putCall) | putCall == '') & shrsOrPrnAmt != 'PRN' & !is.na(prc) & !is.na(shares) & shares != 0, ]

# aggregate all holdings over cik, rdate, fdate, form, permno, address
holdings <- holdings[, list(shares = sum(na.omit(shares)), value = sum(na.omit(value)), prc = prc[1], 
                            shrout = shrout[1], split = split[1], type = type[1]), 
                     by = list(cik, rdate, fdate, form, permno, cusip8, address)]

# re-compute deviation
holdings[, deviation := value / (prc * shares)]

# get adjusted deviation for different reporting units
adjust.col <- function(col, deviation, convert = 0) {
    # compute sum of deviations in all sets
    sevens <- sum(deviation >= 0.00000009 & deviation <= 0.00000011)
    sixes <- sum(deviation >= 0.0000009 & deviation <= 0.0000011)
    fives <- sum(deviation >= 0.000009 & deviation <= 0.000011)
    fours <- sum(deviation >= 0.00009 & deviation <= 0.00011)
    threes <- sum(deviation >= 0.0009 & deviation <= 0.0011)
    twos <- sum(deviation >= 0.009 & deviation <= 0.011)
    ones <- sum(deviation >= 0.09 & deviation <= 0.11)
    zeros <- sum(deviation >= 0.9 & deviation <= 1.1)
    mones <- sum(deviation >= 9 & deviation <= 11)
    mtwos <- sum(deviation >= 90 & deviation <= 110)

    # stack them into a vector
    inds <- c(mtwos, mones, zeros, ones, twos, threes, fours, fives, sixes, sevens)

    # get which occurs most minus 3, multiply col by 10^(that amount)
    out <- col * 10^(which.max(inds) - 3 + convert)

    # return
    return(out)
}
holdings[, deviation_adjusted := adjust.col(deviation, deviation), by = list(cik, address)]

# compute percent within 10% of deviation per report
holdings[, report_quality := sum(deviation_adjusted >= 0.9 & deviation_adjusted <= 1.1, na.rm = TRUE) / sum(!is.na(prc)), 
         by = list(cik, address)]

# get quality as a standalone
quality <- unique(holdings[, c('cik', 'address', 'type', 'report_quality')])

# only keep reports above threshold first percentile of XML filings (48.4%)
quality_threshold <- quantile(quality[type == 'xml', ]$report_quality, 0.01, na.rm = TRUE)
holdings <- holdings[report_quality >= quality_threshold, ]

# only keep observations with rdates on months 3, 6, 9, 12
holdings <- holdings[substr(rdate, 5, 6) %in% c('03', '06', '09', '12'), ]

# make a within 10 percent variable
holdings[, within_10 := deviation_adjusted >= 0.9 & deviation_adjusted <= 1.1]

# compute percent ownership, only keep observations with ownership below the 99.95 percentile of XML filings (38.8%)
holdings[, ownership := shares / (shrout*1000)]
ownership_threshold <- quantile(holdings[type == 'xml', ]$ownership, 0.9995, na.rm = TRUE)
holdings <- holdings[ownership <= ownership_threshold, ]

# keep all observations with a maximum error below the 99 percentile of XML filings (99%)
deviation_threshold <- quantile(abs(holdings[type == 'xml', ]$deviation_adjusted - 1), 0.99, na.rm = T)
holdings <- holdings[abs(deviation_adjusted - 1) <= deviation_threshold, ]

# keep all observations with the maximum value of within_10 by cik, rdate, permno
holdings <- holdings[holdings[, .I[within_10 == max(within_10)], by = list(cik, rdate, permno)]$V1]

# finally, sort by cik, permno, rdate, fdate; take last value
holdings <- holdings[holdings[, .I[.N], by = list(cik, rdate, permno)]$V1]

# make accession variable
holdings[, accession := gsub('(.*/)(.*)(\\.txt)', '\\2', address)]

# convert all value rows to be in thousands
holdings[, value_adjusted := adjust.col(col = value, deviation = deviation, convert = -3), by = list(cik, address)]

# only keep necessary cols, change value_adjusted name to value
holdings <- holdings[, c('cik', 'rdate', 'fdate', 'form', 'permno', 'shares', 'value_adjusted', 'accession')]
colnames(holdings)[colnames(holdings) == 'value_adjusted'] <- 'value'

# format rdate, fdate as dates
holdings[, `:=`(rdate = as.Date(rdate, format = '%Y%m%d'), 
                fdate = as.Date(fdate, format = '%Y%m%d'))]

# save
fwrite(holdings, 'final_tables/holdings.csv')

##########

##### FORMAT BIOGRAPHICAL #####

# make biographical data
biographical <- rbindlist(bios)

# add has_table flag to quality
quality$has_table <- TRUE

# merge in quality information
biographical <- merge(biographical, quality, by.x = c('cik_row', 'address'), by.y = c('cik', 'address'), all.x = TRUE)
biographical[is.na(has_table), has_table := FALSE]

# occassionally, a given filing (address, cik) has multiple filing dates/ciks, etc. take the one that matches
biographical[, cik_match := as.numeric(cik) == as.numeric(cik_row)]
biographical <- unique(biographical[biographical[, .I[cik_match == max(cik_match)], by = address]$V1])

# only keep certain cols
biographical <- biographical[, c('cik_row', 'cikname_row', 'rdate', 'fdate', 'form', 'street1', 'street2', 'city', 
                                 'state', 'zip', 'phone', 'accession', 'type', 'report_quality')]
colnames(biographical) <- gsub('_row', '', colnames(biographical))

# make rdate, fdate dates
biographical[, `:=`(rdate = as.Date(rdate, format = '%Y%m%d'), 
                    fdate = as.Date(fdate, format = '%Y%m%d'))]

# format phone numbers
biographical[, phone := gsub(' |-|,|\\.|\\(|\\)|/|\\#|\\+|[[:alpha:]]', '', phone)]

# save biographical
fwrite(biographical, 'final_tables/biographical.csv')

##########
