###########################################
### functions_download.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/28/20              ###
### Date modified: 03/28/20             ###
###########################################

###########################################################################################################
### This file contains an R function to download 13F filings from EDGAR for a single CIK.               ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_download.R', echo = TRUE)

##### FUNCTION TO DOWNLOAD 13F FILINGS #####

# function to download 13Fs for a single CIK
download.one.cik <- function(table, output.folder, overwrite = FALSE, sleep = 0, min.date = as.Date('1800-01-01'), 
                             max.date = Sys.Date(), base.url = 'https://www.sec.gov/Archives/edgar/data', 
                             counter, total, start.time) {
    # first, create main folder if it doesn't exist
    folder <- paste(output.folder, unique(table$cik), sep = '/')
    if(!dir.exists(folder)) dir.create(folder, recursive = TRUE)

    # subset by fdate
    table <- table[fdate >= min.date & fdate <= max.date, ]

    # if overwrite = TRUE, replace all files
    if(overwrite == TRUE) {
        files.to.download <- table$address
    } else {
        files.to.download <- table$address[!file.exists(paste(output.folder, table$address, sep = '/'))]
    }

    # iterate over files to download (if length > 0)
    if(length(files.to.download) > 0) {
        for(i in 1:length(files.to.download)) {
            # pause for a random number of seconds between 1 and "sleep" to prevent over loading edgar
            # EDGAR prevents more than 10 requests per second, set sleep based on parallelization
            if(sleep >= 1) Sys.sleep(sample(1:sleep, 1))

            # get file
            txt <- readLines(paste(base.url, files.to.download[i], sep = '/'))

            # save file
            writeLines(txt, paste(output.folder, files.to.download[i], sep = '/'))
        }
    }

    # print progress if counter and total supplied
    if(!missing(counter) & !missing(total)) {
        if(missing(start.time)) progress(counter, total, message = 'Downloaded files')
        if(!missing(start.time)) progress(counter, total, start.time, message = 'Downloaded files')
    }
}

##########
