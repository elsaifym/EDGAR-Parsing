###########################################
### functions_parse.R                   ###
### Author: Morad Elsaify               ###
### Date created: 03/15/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file contains R functions to parse the information tables extracted from 4. extract_13f_data.R ###
### into relevant columns (most importantly, cusip, value, shares). For XML filings, this is fairly     ###
### straightforward. For filings prior to 2013Q2 (after which XML filings became mandated), the filings ###
### are remarkably heterogeneous in their formatting. These functions are general enough to deal with   ###
### the vast majority of these filings, although there are some errors.                                 ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/Functions/functions_parse.R', echo = TRUE)

##### FUNCTIONS TO SEPARATE ROWS INTO COLUMNS SEPARATE DATA FIELDS #####

# function to split a row into cusip and its remainder
cusip.remainder <- function(table, cusip.universe) {
    # get entire 9 digits (6 digit cusip match + 3 extra letters/numbers) and indices
    out <- regmatches.multiple(cusip.universe, table, max.digits = 9, n = 100, ignore.case = TRUE)
    cusip9 <- toupper(out$match)
    end <- out$end

    # now, get shrsOrPrnAmt
    shrsOrPrnAmt <- rep(as.character(NA), length(cusip9))
    shrsOrPrnAmt[grepl('SH', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'SH'
    shrsOrPrnAmt[grepl('PRN|PRIN', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'PRN'

    # get putCall items
    putCall <- rep(as.character(NA), length(cusip9))
    putCall[grepl('PUT', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'PUT'
    putCall[grepl('CALL', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'CALL'
    
    # get investment discretion
    investmentDiscretion <- rep(as.character(NA), length(cusip9))
    investmentDiscretion[grepl('SOLE', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'SOLE'
    investmentDiscretion[grepl('OTHER|OTR|OTH', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'OTR'
    investmentDiscretion[grepl('DEFINED|DFND|DEF', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'DFND'
    investmentDiscretion[grepl('SHARED', substr(table, end, nchar(table)), ignore.case = TRUE)] <- 'SHARED'
    
    # get remainder of strings, identify endings
    strings.to.cut <- c('SH', 'PRN', 'PRIN', 'CALL', 'PUT', 'SOLE', 'OTHER', 'OTR', 'OTH', 'DEFINED', 'DFND', 'DEF', 
                        'SHARED', 'YES')
    regex <- paste0('(^|[^[:alpha:]]{1})(', paste0(strings.to.cut, collapse = '|'), ')([^[:alpha:]]{1}|$)')
    remainder.end <- regexec(regex, substr(table, end, nchar(table)), ignore.case = TRUE)

    # get start index of second group (the strings.to.cut)
    remainder.end <- unlist(lapply(remainder.end, function(x) ifelse(length(x) < 3, NA, x[[3]])))
    remainder.end <- ifelse(is.na(remainder.end), nchar(table), end - 2 + remainder.end)

    # return cusip and remainder
    return(list(cusip9 = cusip9, cusip8 = substr(cusip9, 1, 8), shrsOrPrnAmt = shrsOrPrnAmt, putCall = putCall, 
                investmentDiscretion = investmentDiscretion, remainder = substr(table, end, remainder.end)))
}

# function to separate the remainder into groups of numbers
separate.remainder <- function(rows, regex = '(\\d+|\\d{1,3}(,\\d{3})*)(\\.\\d*)?') {
    # NOTE: default regex matches all numbers (with or without commas every three digits, and all digits after a decimal)
    
    # extract numbers, remove commas, convert to numeric
    numbers <- regmatches(rows, gregexpr(regex, rows))
    numbers <- lapply(numbers, function(x) as.list(as.numeric(gsub(',', '', x))))

    # give numbers names
    assign.names <- function(x) {
        # fill x with NA is all missing
        if(length(x) == 0) {
            x <- list(NA)
        }

        # assign names, return x
        names(x) <- paste0('num', 1:length(x))
        return(x)
    }
    numbers <- lapply(numbers, assign.names)

    # get into data.table, return
    numbers <- rbindlist(numbers, fill = TRUE)
    colnames(numbers) <- paste0('num', 1:ncol(numbers))
    return(numbers)
}

##########

##### FUNCTION TO DETERMINE WHICH NUMERIC COLUMNS ARE VALUES VS. SHARES USING CRSP DATA #####

# function to determine cols
determine.cols <- function(data) {
    # if no numbers, return data
    if(sum(grepl('^num', colnames(data))) == 0) {
        warning('No value or shares columns.', immediate. = TRUE)
        return(data)
    }

    # if only one number, return shares
    if(sum(grepl('^num', colnames(data))) == 1) {
        colnames(data)[grepl('^num', colnames(data))] <- 'shares'
        warning('No value column.', immediate. = TRUE)
        return(data)
    }

    # get all permutations of colnames starting with 'num'
    vars <- combn(colnames(data)[grepl('^num', colnames(data))], 2)
    vars <- cbind(vars, vars[2:1, ])
    rownames(vars) <- c('value', 'shares')

    # calculate all value / prc * shrs, get max number of non-missings
    results <- lapply(1:ncol(vars), function(x) get(vars[1, x], data) / (get(vars[2, x], data) * get('prc', data)))
    max.nonmissing <- max(unlist(lapply(results, function(x) sum(!is.na(x)))))

    # determine which is closest to what it should be (between 0.0009 and 0.0011 or between 0.9 and 1.1)
    compute.stat <- function(x) {
        return(max(sum(x >= 0.0009 & x <= 0.0011, na.rm = TRUE), sum(x >= 0.9 & x <= 1.1, na.rm = TRUE)) / max.nonmissing)
    }
    closest.ind <- which.max(lapply(results, compute.stat))

    # if all NA, set shares to second column and value to first column
    if(length(closest.ind) == 0) {
        warning('No CRSP data to validate columns. Assuming first number is value, second is shares')
        closest.ind <- 1
    }

    # finally, assign column names
    colnames(data)[grepl(vars[1, closest.ind], colnames(data))] <- names(vars[1, closest.ind])
    colnames(data)[grepl(vars[2, closest.ind], colnames(data))] <- names(vars[2, closest.ind])

    # add column of deviation
    data$deviation <- data$value / (data$shares * data$prc)
    
    # return data
    return(data)
}

##########

##### FUNCTIONS TO PARSE FILINGS DEPENDING ON FILING TYPE (XML, FWF, TAB, CSV) #####

# function to parse xml tables
parse.xml <- function(table, cusip.universe, crsp.universe) {

    # get data
    data <- as.data.table(xmlToDataFrame(xmlParse(table), stringsAsFactors = FALSE))

    # make cusip8, cusip9 variables
    data[, `:=`(cusip9 = substr(cusip, 1, 9), cusip8 = substr(cusip, 1, 8))]
    data <- data[, -'cusip']

    # make variable if in sample
    data[, in_universe := grepl.multiple(cusip.universe, cusip9, n = 100, ignore.case = TRUE)]

    # add value numeric, shares, make shrsOrPrnAmt 
    data[, value := as.numeric(gsub('\n|\t| ', '', value))]
    data[, shares := as.numeric(gsub('\n|\t|SH|PRN| ', '', shrsOrPrnAmt))]
    data[, shrsOrPrnAmt := ifelse(grepl('SH', shrsOrPrnAmt), 'SH', 
                                  ifelse(grepl('PRN', shrsOrPrnAmt), 'PRN', as.character(NA)))]

    # merge to crsp
    data <- merge(data, crsp.universe, by.x = c('cusip8'), by.y = c('ncusip'), all.x = TRUE)[, -c('yearqtr')]

    # add deviation column
    data[, deviation := value / (prc * shares)]

    # return out
    return(data)
}

# function to parse fixed width tables
parse.fwf <- function(table, cusip.universe, crsp.universe, ...) {
    # apply cusip.remainder over table
    data <- as.data.table(do.call(cbind, cusip.remainder(table, cusip.universe = cusip.universe)))

    # separate data
    numbers <- separate.remainder(data$remainder, ...)

    # combine all data
    data <- cbind(data, numbers)

    # merge to crsp
    data <- merge(data, crsp.universe, by.x = c('cusip8'), by.y = c('ncusip'), all.x = TRUE)[, -c('yearqtr')]

    # determine cols, return data
    data <- determine.cols(data)
    return(data)
}

# function to parse tab separated tables
parse.tab <- function(table, cusip.universe, crsp.universe, ...) {
    # substitute all '\t' with four spaces
    table <- gsub('\t', '    ', table)

    # apply cusip.remainder over table
    data <- as.data.table(do.call(cbind, cusip.remainder(table, cusip.universe = cusip.universe)))

    # separate data
    numbers <- separate.remainder(data$remainder, ...)

    # combine all data
    data <- cbind(data, numbers)

    # merge to crsp
    data <- merge(data, crsp.universe, by.x = c('cusip8'), by.y = c('ncusip'), all.x = TRUE)[, -c('yearqtr')]

    # determine cols, return data
    data <- determine.cols(data)
    return(data)
}

# function to parse csv tables
parse.csv <- function(table, cusip.universe, crsp.universe, ...) {
    # remove all commas between quotes
    quoted <- regmatches(table, gregexpr('(\")(.*?)(\")', table))
    quoted_nocomma <- lapply(quoted, function(x) gsub('\"|,', '', x))
    table <- lapply(1:length(table), function(i) gsub.all(pattern = quoted[[i]], replacement = quoted_nocomma[[i]], 
                                                          x = table[i]))

    # now, subsitute all commas and '\t' with four spaces
    table <- gsub('\t|,', '    ', table)
    
    # apply cusip.remainder over table
    data <- as.data.table(do.call(cbind, cusip.remainder(table, cusip.universe = cusip.universe)))

    # separate data
    numbers <- separate.remainder(data$remainder, ...)

    # combine all data
    data <- cbind(data, numbers)

    # merge to crsp
    data <- merge(data, crsp.universe, by.x = c('cusip8'), by.y = c('ncusip'), all.x = TRUE)[, -c('yearqtr')]

    # determine cols, return data
    data <- determine.cols(data)
    return(data)
}

##########

##### WRAPPERS TO PARSE A SINGLE TABLE AND ALL TABLES #####

# function to parse a single table
parse.one.table <- function(table, tabletype = c('xml', 'fwf', 'tab', 'csv', 'none'), biographical.row, 
                            cusip.universe.all, crsp.universe.all, counter, total, start.time) {

    # first, subset cusip.universe.all, crsp.universe.all to relevant rdate
    cusip.universe <- cusip.universe.all[[paste0('y', substr(biographical.row$rdate[1], 1, 4), 'q', 
                                                 ceiling(as.numeric(substr(biographical.row$rdate[1], 5, 6)) / 3))]]
    crsp.universe <- crsp.universe.all[yearqtr == as.yearqtr(paste0(substr(biographical.row$rdate[1], 1, 4), ' Q', 
                                                                    as.numeric(substr(biographical.row$rdate[1], 5, 6)) / 3)), ]

    # get proper function
    functions <- list(xml = parse.xml, fwf = parse.fwf, tab = parse.tab, csv = parse.csv, none = function(x) return(NULL))
    tabletype <-  match.arg(tabletype, tabletype, several.ok = FALSE)
    parse.function <- functions[[tabletype]]

    # parse and catch errors and warnings
    final <- tryCatch.W.E(parse.function(table, cusip.universe, crsp.universe))

    # add relevant columns--address, rdate, fdate, cik_row, form
    final$value$address <- biographical.row$address
    final$value$rdate <- biographical.row$rdate
    final$value$fdate <- biographical.row$fdate
    final$value$cik <- biographical.row$cik_row
    final$value$form <- biographical.row$form

    # print progress if counter and total supplied
    if(!missing(counter) & !missing(total)) {
        progress(counter, total, start.time, message = 'Parsed tables from')
    }

    # return info
    return(list(table = final$value, warning = final$warning, error = final$error))
}

# function to parse all tables
parse.all.tables <- function(tablist, cusip.universe.all, crsp.universe.all, num.cores = detectCores(), ...) {

    # require XML package
    require(XML)

    # get start time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        tables <- lapply(1:length(tablist), 
                         function(i) parse.one.table(table = tablist[[i]]$tableinfo$table, 
                                                     tabletype = tablist[[i]]$tableinfo$type, 
                                                     biographical.row = tablist[[i]]$biographical[1, ], 
                                                     cusip.universe.all = cusip6_universe,
                                                     crsp.universe.all = crspq, 
                                                     counter = i, total = length(tablist), start.time = start.time))
    } else {
        tables <- mclapply(1:length(tablist), 
                           function(i) parse.one.table(table = tablist[[i]]$tableinfo$table, 
                                                       tabletype = tablist[[i]]$tableinfo$type, 
                                                       biographical.row = tablist[[i]]$biographical[1, ], 
                                                       cusip.universe.all = cusip6_universe,
                                                       crsp.universe.all = crspq, 
                                                       counter = i, total = length(tablist), start.time = start.time), 
                           mc.cores = num.cores, ...)
    }

    # return tables
    return(tables)
}

##########