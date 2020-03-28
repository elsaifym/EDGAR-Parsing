###########################################
### functions_download.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/15/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file contains R functions to download the 13F filings from EDGAR. This file is sourced in      ###
### 3-download_13f_data.R.                                                                              ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_download.R', echo = TRUE)

##### FUNCTIONS TO DOWNLOAD 13F FILINGS #####

# function to download single 13F
download.one.13f <- function(row, counter, total, start.time, base.url = 'https://www.sec.gov/Archives/') {

    # set location, create directory if necessary
    location <- unlist(strsplit(row$address, '/'))
    location <- paste(location[-length(location)], collapse = '/')
    if(!dir.exists(location)) dir.create(location)

    # if file does not exist, load and save
    if(!file.exists(row$address)) {
        # get file
        txt <- readLines(paste0(base.url, row$address))
        writeLines(txt, row$address)
    }

    # print progress if counter and total supplied
    progress(counter, total, start.time, message = 'Downloaded files')
}

# wrapper to lapply over all 13Fs
download.all.13f <- function(table, start, end, min.date = as.Date('2013-09-30'), max.date = as.Date(Sys.time()), 
                             num.cores = detectCores(), ...) {

    # create initial directory
    if(!dir.exists('edgar/')) dir.create('edgar/')
    if(!dir.exists('edgar/data')) dir.create('edgar/data')

    # subset based on min.date, max.date
    if(!missing(min.date)) table <- table[fdate >= min.date, ]
    if(!missing(max.date)) table <- table[fdate >= max.date, ]

    # get start, end if missing
    if(missing(start)) start <- 1
    if(missing(end)) end <- nrow(table)
    
    # get start time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        results <- lapply(start:end, function(i) download.one.13f(table[i, ], i - start + 1, end - start + 1, start.time, ...))
    } else {
        results <- mclapply(start:end, function(i) download.one.13f(table[i, ], i - start + 1, end - start + 1, start.time, ...), 
                            mc.cores = num.cores)
    }
}

##########
