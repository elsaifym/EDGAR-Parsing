###########################################
### 4. extract_13f_data.R               ###
### Author: Morad Elsaify               ###
### Date created: 02/04/19              ###
### Date modified: 03/26/19             ###
###########################################

###########################################################################################################
### This file extracts relevant information (biographical info and information tables) from form 13F-HR ###
### 13F-HR/A, 13F-NT, and 13F-NT/A beginning in all EDGAR filings (starting in ~1999). The function     ###
### uses the downloaded 13F reports from 3. download_13f_data.R and crawls the .txt filings to extract  ###
### the information.                                                                                    ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/4. extract_13f_data.R', echo = TRUE)

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
source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_general.R')
source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_extract.R')

# set directory
setwd('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Data')

# load cusip6s
load('CUSIP Universe/cusip6_universe.Rdata')

##### GATHER ALL REPORTS WITH INFO WE WANT TO EXTRACT #####

# get addresses
addresses <- get.all.addresses(folder = 'Master Files/All_13F', min.date = as.Date('1980-01-01'))
addresses <- addresses[order(cik, fdate, form), ]

##########

##### GATHER INFOTABLES #####

# extract all 13f information (split up into calls of 1000 for runtime and debugging purposes)
n <- 1000
out <- as.list(rep(NA, ceiling(nrow(addresses) / n)))
for(i in 1:ceiling(nrow(addresses) / n)) {
    # gather temp until all are length of 3
    mistake <- TRUE
    j <- 1
        
    while(mistake) {
        cat('Attempt', j, 'in set', i, 'of', ceiling(nrow(addresses) / n), '\n')

        # get data
        temp <- extract.all.13f(addresses[(1 + (i-1)*n):min(nrow(addresses), (n*i)), ])

        # check to see if there was a mistake, if not, change mistake to FALSE
        if(any(unlist(lapply(temp, length)) != 2)) {
            warning(paste('Mistake in attempt', j, 'in set', i, 'of', ceiling(nrow(addresses) / n)), immediate. = TRUE)
            j <- j + 1
        } else {
            mistake <- FALSE
        }
    }

    # save temp
    saveRDS(temp, file = paste0('Table Rows/tables_', i, '.rds'))

    # store temp in out
    out[[i]] <- temp
    cat(paste('Done', i, 'of', ceiling(nrow(addresses) / n), '! \n'))
}

##########
