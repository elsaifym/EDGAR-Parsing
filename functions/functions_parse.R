###########################################
### functions_parse.R                   ###
### Author: Morad Elsaify               ###
### Date created: 03/28/20              ###
### Date modified: 03/30/20             ###
###########################################

###########################################################################################################
### This file contains R functions to parse the information tables extracted from 13F filings into      ###
### relevant columns (most importantly, cusip, value, and shares). For XML filings, this is fairly      ###
### straightforward. For filings prior to 2013Q2 (after which XML filings became mandated), the filings ###
### are remarkably heterogeneous in their formatting. These functions are general enough to deal with   ###
### the vast majority of these filings, although some filings cannot be easily parsed.                  ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_parse.R', echo = TRUE)

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
    data[, `:=`(cusip9 = substr(toupper(cusip), 1, 9), cusip8 = substr(toupper(cusip), 1, 8))]
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

# function to parse all other tables
parse.others <- function(table, cusip.universe, crsp.universe) {

    # apply cusip.remainder over table
    data <- as.data.table(do.call(cbind, cusip.remainder(table, cusip.universe = cusip.universe)))

    # separate data
    numbers <- separate.remainder(data$remainder)

    # combine all data
    data <- cbind(data, numbers)

    # merge to crsp
    data <- merge(data, crsp.universe, by.x = c('cusip8'), by.y = c('ncusip'), all.x = TRUE)[, -c('yearqtr')]

    # determine cols, return data
    data <- determine.cols(data)
    return(data)
}

##########

##### FUNCTION TO PARSE ANY ONE TABLE #####

# function to parse any given table
parse.one.table <- function(row, table.input.folder, biographical.input.folder, cusip.universe.all, 
                            crsp.universe.all, output.folder, error.file, overwrite = FALSE, counter, total, start.time) {

    # require XML
    require(XML)

    # load biographical data, raw table
    biographical <- fread(paste(biographical.input.folder, paste0(row$cik, '.csv'), sep = '/'), header = TRUE, sep = ',')
    biographical <- biographical[address == row$address, ]
    table <- readLines(paste(table.input.folder, row$address, sep = '/'))

    # get type
    type <- determine.type(table)

    # subset cusip.universe.all, crsp.universe.all to relevant rdates
    cusip.universe <- cusip.universe.all[[paste0('y', substr(biographical$rdate[1], 1, 4), 'q', 
                                                 ceiling(as.numeric(substr(biographical$rdate[1], 5, 6)) / 3))]]
    crsp.universe <- crsp.universe.all[yearqtr == as.yearqtr(as.Date(as.character(biographical$rdate[1]), 
                                                                     format = '%Y%m%d'))]

    # if cleaned table does not exist, create it
    if(overwrite | !file.exists(paste(output.folder, gsub('.txt', '.csv', row$address), sep = '/'))) {

        # if type is none, skip
        if(type == 'none') {
            return()
        }

        # convert tabs and csvs to fwfs
        if(type == 'tab') {
            # substitute '\t' with four spaces--effectively convert to fwf
            table <- gsub('\t', '    ', table)
        } else if(type == 'csv') {
            # remove all commas between quotes
            quoted <- regmatches(table, gregexpr('(\")(.*?)(\")', table))
            quoted_nocomma <- lapply(quoted, function(x) gsub('\"|,', '', x))
            table <- lapply(1:length(table), function(i) gsub.all(pattern = quoted[[i]], replacement = quoted_nocomma[[i]], 
                                                                  x = table[i]))

            # now, subsitute all commas and '\t' with four spaces
            table <- gsub('\t|,', '    ', table)
        }

        # convert to table
        if(type == 'xml') {
            # parse xml table
            out <- tryCatch.W.E(parse.xml(table, cusip.universe, crsp.universe))
        } else if(type %in% c('fwf', 'tab', 'csv')) {
            # parse fwf table
            out <- tryCatch.W.E(parse.others(table, cusip.universe, crsp.universe))
        }

        # add relevant columns--address, rdate, fdate, cik_row, form, type
        out$value$address <- biographical$address[1]
        out$value$rdate <- biographical$rdate[1]
        out$value$fdate <- biographical$fdate[1]
        out$value$cik <- biographical$cik_row[1]
        out$value$form <- biographical$form[1]
        out$value$type <- biographical$type[1]

        # save table (create directory if it does not exist)
        if(!dir.exists(paste(output.folder, biographical$cik_row[1], sep = '/'))) {
            dir.create(paste(output.folder, biographical$cik_row[1], sep = '/'))
        }
        fwrite(out$value, paste(output.folder, gsub('.txt', '.csv', biographical$address[1]), sep = '/'))

        # save warnings/errors
        if(length(as.character(out$warning)) > 0 | length(as.character(out$error)) > 0) {
            # get warnings and errors
            warning <- ifelse(is.null(as.character(out$warning)), '', as.character(out$warning))
            error <- ifelse(length(as.character(out$error)) == 0, '', as.character(out$error))

            # if file exists, do not use colnames
            write.table(data.table(cik = biographical$cik[1], accession = biographical$accession[1], 
                                   warning = warning, error = error), 
                        file = error.file, 
                        sep = ',', row.names = FALSE, 
                        append = file.exists(error.file), 
                        col.names = !file.exists(error.file))
        }

        # print progress if counter and total supplied
        if(!missing(counter) & !missing(total)) {
            if(missing(start.time)) progress(counter, total, message = 'Parsed Tables from')
            if(!missing(start.time)) progress(counter, total, start.time, message = 'Parsed Tables from')
        }

        # return table
        return(out$value)    
    }
}

##########