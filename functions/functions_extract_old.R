###########################################
### functions_extract.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/15/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file contains R functions to extract biographical information (CIK, name, report date, etc.)   ###
### and the information table for the downloaded 13F filings. The file is sourced in                    ###
### 4. extract_13f_data.R.                                                                              ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_extract.R', echo = TRUE)

##### FUNCTIONS TO EXTRACT BIOGRAPHICAL INFORMATION #####

# function to get a single set of biographical info
get.one.biographical <- function(short) {
    # initialize output
    info <- list(cikname = NA, cik = NA, street1 = NA, street2 = NA, city = NA, state = NA, zip = NA, phone = NA)

    # get cikname, cik
    info[['cikname']] <- gsub('COMPANY CONFORMED NAME:', '', gsub('\\t', '', short[grepl('COMPANY CONFORMED NAME:', short)]))
    info[['cik']] <- gsub('CENTRAL INDEX KEY:', '', gsub('\\t', '', short[grepl('CENTRAL INDEX KEY:', short)]))

    # get bus address information
    start <- max(1, which(grepl('BUSINESS ADDRESS:', short)))
    end <- min(which(grepl('MAIL ADDRESS:', short)), length(short))
    busaddr <- short[(start+1):(end-1)]

    # extract bus addr info
    info[['street1']] <- gsub('STREET 1:', '', gsub('\\t', '', busaddr[grepl('STREET 1:', busaddr)]))
    info[['street2']] <- gsub('STREET 2:', '', gsub('\\t', '', busaddr[grepl('STREET 2:', busaddr)]))
    info[['city']] <- gsub('CITY:', '', gsub('\\t', '', busaddr[grepl('CITY:', busaddr)]))
    info[['state']] <- gsub('STATE:', '', gsub('\\t', '', busaddr[grepl('STATE:', busaddr)]))
    info[['zip']] <- gsub('ZIP:', '', gsub('\\t', '', busaddr[grepl('ZIP:', busaddr)]))
    info[['phone']] <- gsub('BUSINESS PHONE:', '', gsub('\\t', '', busaddr[grepl('BUSINESS PHONE:', busaddr)]))

    # if missing (character(0)), set to NA
    info <- lapply(info, function(x) ifelse(length(x) == 0, NA, x))

    return(info)
}

# function to gather all biographical info of a single filing
get.biographical <- function(txt, row) {

    # get first of form type, rdate, fdate
    # info that appears multiple times the result of resubmissions/amendments, first is most accurate
    form <- gsub('CONFORMED SUBMISSION TYPE:', '', gsub('\\t', '', txt[grepl('CONFORMED SUBMISSION TYPE:', txt)]))[1]
    rdate <- gsub('CONFORMED PERIOD OF REPORT:', '', gsub('\\t', '', txt[grepl('CONFORMED PERIOD OF REPORT:', txt)]))[1]
    fdate <- gsub('FILED AS OF DATE:', '', gsub('\\t', '', txt[grepl('FILED AS OF DATE:', txt)]))[1]

    # now, restrict text to that between start and first '</SEC-HEADER'
    end <- min(which(grepl('</SEC-HEADER>', txt)))
    txt.sub <- txt[1:end]
    
    # get indices of filers
    filer.inds <- c(which(grepl('FILER:', txt.sub)), end)

    # iterate over filer.inds
    biographical.txt <- lapply(1:(length(filer.inds) - 1), 
                               function(i) get.one.biographical(txt.sub[filer.inds[i]:(filer.inds[i+1] - 1)]))

    # rbindlist biographical.txt
    biographical.txt <- rbindlist(biographical.txt)

    # add form, rdate, fdate
    biographical.txt$form <- form
    biographical.txt$rdate <- rdate
    biographical.txt$fdate <- fdate

    # add address, cik_row, cikname_row, fdate_row
    biographical.txt$address <- row$address
    biographical.txt$cik_row <- row$cik
    biographical.txt$cikname_row <- row$cikname
    biographical.txt$fdate_row <- row$fdate

    # return biographical.txt
    return(biographical.txt)
}

##########

##### FUNCTION TO EXTRACT THE INFORMATION TABLE AND DETECT ITS TYPE #####

# function to extract and detect type of information table
detect.infotable <- function(txt, rdate, cusips, start.rdate = 20000000, numseps = 3, minspace = 3, min.frac = 0.5) {

    # set table to NULL, type to none
    table <- NULL
    type <- 'none'

    # XML format contains <informationTable or :informationTable
    if(any(grepl('<informationTable|:informationTable', txt))) {
        type <- 'xml'

        # get indices
        start <- min(which(grepl('<informationTable', txt) | grepl(':informationTable', txt)))
        end <- max(which(grepl('</informationTable', txt) | grepl(':informationTable', txt)))

        # get table
        table <- txt[start:end]
    } else if(as.numeric(rdate) >= start.rdate) {
        # start table after </SEC-HEADER>
        txt.post <- txt[max(which(grepl('</SEC-HEADER>', txt))):length(txt)]

        # get rows with the correct cusips
        uni <- cusips[[paste0('y', substr(rdate, 1, 4), 'q', ceiling(as.numeric(substr(rdate, 5, 6)) / 3))]]
        table <- txt.post[grepl.multiple(uni, txt.post, n = 100, ignore.case = TRUE)]

        # remove common error rows
        error.rows <- c('<SEC-DOCUMENT>', '<cik>', '<SEC-HEADER>', 'ACCESSION NUMBER:', '<ACCEPTANCE-DATETIME>', 
                        '<FILENAME>', '<phone>', '</additionalInformation>')
        table <- table[!grepl(paste0(error.rows, collapse = '|'), table, ignore.case = TRUE)]

        # now, determine type
        tabseps <- sum(unlist(lapply(strsplit(table, '\t'), function(x) length(x) - 1)) >= numseps) / length(table)
        comseps <- sum(unlist(lapply(strsplit(table, '\\,'), function(x) length(x) - 1)) >= numseps) / length(table)
        spaceseps <- sum(unlist(lapply(gregexpr('[[:alnum:]]\\s+', table), 
                                       function(x) max(attr(x, 'match.length')))) >= minspace) / length(table)

        # if tabseps > min.frac, set to tab, if comseps > min.frac, set to csv
        if(comseps >= min.frac & !is.na(comseps)) type <- 'csv'
        if(spaceseps >= min.frac & !is.na(spaceseps)) type <- 'fwf'
        if(tabseps >= min.frac & !is.na(tabseps)) type <- 'tab'

        # if table has 0 rows, set type to none
        if(length(table) == 0) type <- 'none'
    }

    # return table, type
    return(list(table = table, type = type)) 
}

##########

##### FUNCTIONS TO EXTRACT 13F BIOGRAPHICAL DATA AND INFORMATION TABLE #####

# function to extract info from one 13f
extract.one.13f <- function(row, counter, total, start.time, base.address = NULL) {

    # get file
    txt <- readLines(paste(c(base.address, row$address), collapse = '/'))

    # get biographical data
    biographical <- get.biographical(txt, row)

    # detect infotable and its type
    tableinfo <- detect.infotable(txt, biographical$rdate[1], cusips = cusip6_universe)

    # print progress if counter and total supplied
    if(!missing(counter) & !missing(total)) {
        progress(counter, total, start.time, message = 'Extracted tables from')
    }

    return(list(biographical = biographical, tableinfo = tableinfo))
}

# function to extract info fromall 13fs
extract.all.13f <- function(addresses, min.date, max.date, num.cores = detectCores(), ...) {

    # subset based on min.date, max.date
    if(!missing(min.date)) addresses <- addresses[fdate >= min.date, ]
    if(!missing(max.date)) addresses <- addresses[fdate >= max.date, ]

    # get start time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        results <- lapply(1:nrow(addresses), function(i) extract.one.13f(addresses[i, ], i, nrow(addresses), start.time, ...))
    } else {
        results <- mclapply(1:nrow(addresses), function(i) extract.one.13f(addresses[i, ], i, nrow(addresses), start.time, ...), 
                            mc.cores = num.cores)
    }

    # return results
    return(results)
}

##########
