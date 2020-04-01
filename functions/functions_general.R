###########################################
### functions_general.R                 ###
### Author: Morad Elsaify               ###
### Date created: 03/28/20              ###
### Date modified: 03/31/20             ###
###########################################

###########################################################################################################
### This file contains all the generic R functions used to parse the EDGAR database. They include       ###
### functions to handle errors and warnings, generic progress functions, string matching functions, and ###
### a function to get the locations of 13F filings in the EDGAR Archives. This file is sourced in all R ###
### files.                                                                                              ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/functions/functions_general.R', echo = TRUE)

##### FUNCTIONS TO HANDLE ERRORS #####

tryCatch.W.E <- function(expr) {
  # initialize warning, error
  warn <- NULL
  err <- NULL

  # get value
  value <- withCallingHandlers(
    tryCatch(expr, error = function(e) {
      err <<- e
      NULL
    }), warning = function(w) {
      warn <<- w
      invokeRestart("muffleWarning")
    })

  # return value, warning, and error
  list(value=value, warning=warn, error=err)
}

##########

##### GENERIC PROGRESS FUNCTIONS #####

# function to display difftime as HH:MM:SS
hhmmss <- function(diff) {
    return(sprintf('%02d:%02d:%02d', as.numeric(diff) %/% 3600, as.numeric(diff) %% 3600 %/% 60, 
                   as.numeric(diff) %% 60 %/% 1))
}

progress <- function(counter, total, start.time, message) {

    # initialize base message
    message.print <- paste(message, counter, 'out of', paste0(total, '! '))

    # get elapsed time, remaining time if start.time supplied
    if(!missing(start.time)) {
        remaining <- hhmmss((as.numeric(difftime(Sys.time(), start.time, units = 'secs')) / counter) * (total - counter))
        elapsed <- hhmmss(as.numeric(difftime(Sys.time(), start.time, units = 'secs')))

        # add to message.print
        message.print <- paste0(message.print, 'Time elapsed: ', elapsed, '. Estimated time remaining: ', remaining, '.')
    }

    # add newline/sameline
    message.print <- ifelse(counter != total, paste(message.print, '\r'), paste(message.print, '\n'))

    cat(message.print)
}

##########

##### MISCELLANEOUS STRING MATCHING FUNCTIONS #####

# function to quickly find multiple matches (use n ~ 100)
grepl.multiple <- function(pattern, x, n, ...) {
    # initialize matches to false
    matches <- rep(FALSE, length(x))
    len <- length(pattern)

    if(n > 1) {
        for(i in 1:ceiling(length(pattern) / n)) {
            matches <- pmax(matches, grepl(paste0(pattern[(1 + (i - 1)*n):min(i*n, len)], collapse = '|'), x, ...))
        }
    } else {
        for(i in pattern) {
            matches <- pmax(matches, grepl(i, x))
        }
    }
    return(as.logical(matches))
}

# function to quickly find the first isntance of where any of "pattern" substrings occur in a string (use n ~ 100)
regmatches.multiple <- function(pattern, x, n, min.digits = 6, max.digits = 9, ...) {

    # initialize match.inds, get length of pattern
    match.final <- nchar(x) + 1
    match.final.length <- rep(0, length(x))
    len <- length(pattern)

    # look through all matches
    if(n > 1) {
        for(i in 1:ceiling(len / n)) {
            match.temp <- regexpr(paste0(pattern[(1 + (i-1)*n):min(i*n, len)], collapse = '|'), x, ...)
            match.final.length <- ifelse(match.temp < match.final & match.temp > -1, 
                                    attr(match.temp, 'match.length'), match.final.length)
            match.final <- ifelse(match.temp < match.final & match.temp > -1, match.temp, match.final)
        }
    } else {
        for(i in 1:len) {
            match.temp <- regexpr(pattern[i], x, ...)
            match.final.length <- ifelse(match.temp < match.final & match.temp > -1, 
                                    attr(match.temp, 'match.length'), match.final.length)
            match.final <- ifelse(match.temp < match.final & match.temp > -1, match.temp, match.final)
        }
    }

    # now, return match and indices (do not allow for two spaces between cusip digits)
    look.for.cusips <- sapply(strsplit(substr(x, match.final, nchar(x)), '  '), function(x) x[[1]])
    match <- substr(gsub('\\s+|\\-|\\,|\\.|\\(|\\*|\\#|\\$|\\/', '', look.for.cusips), 1, max.digits)
    start <- match.final
    end <- match.final + attr(regexpr(paste0('^([[:alnum:]][ -]*){', min.digits - 1, ',', max.digits - 1, '}[[:alnum:]]'), 
                                      look.for.cusips), 'match.length')

    # return match and indices
    return(list(match = match, start = start, end = end))
}

# function to substitute all
gsub.all <- function(pattern, replacement, x) {
    # if length 0, make no changes
    if(length(pattern) == 0) {
        return(x)
    }

    # else, iterate over pattern
    for(i in 1:length(pattern)) {
        x <- gsub(pattern[i], replacement[i], x)
    }

    # return x
    return(x)
}

##########

##### FUNCTIONS TO GATHER ADDRESSES OF FILINGS #####

# function to gather addresses for a single cik
get.one.address <- function(file.name, folder = 'master_files/all_13f', min.date = as.Date('1800-01-01'), 
                            max.date = Sys.Date(), counter, total, start.time) {
    
    # load file
    file <- fread(paste(folder, file.name, sep = '/'), header = FALSE, 
                  col.names = c('cik', 'cikname', 'form', 'fdate', 'address'), sep = ',')

    # convert date to date type
    file$fdate <- as.Date(file$fdate)

    # remove 'edgar/data/' from address
    file$address <- gsub('edgar/data/', '', file$address)

    # subset to after min.date, before max.date
    file <- file[fdate >= min.date & fdate <= max.date, ]

    # print progress if counter and total supplied
    if(!missing(counter) & !missing(total)) {
        if(missing(start.time)) progress(counter, total, message = 'Extracted 13F data from')
        if(!missing(start.time)) progress(counter, total, start.time, message = 'Extracted 13F data from')
    }

    # return
    return(file)
}

##########
