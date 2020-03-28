###########################################
### functions_parallel.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/26/20              ###
### Date modified: 03/27/20             ###
###########################################

###########################################################################################################
### This file contains all functions necessary to parse the EDGAR filings in parallel using the SLURM   ###
### batch system. All files are similar in nature to functions_general.R, functions_download.R,         ###
### functions_extract.R, and functions_parse.R.                                                         ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_parallel.R', echo = TRUE)

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

# function to gather addresses in a specific file
get.one.address <- function(file.name, folder = 'master_files/all_13f', 
                            min.date = as.Date('1800-01-01'), max.date = Sys.Date()) {
    
    # load file
    file <- fread(paste(folder, file.name, sep = '/'), header = FALSE, 
                  col.names = c('cik', 'cikname', 'form', 'fdate', 'address'), sep = ',')

    # convert date to date type
    file$fdate <- as.Date(file$fdate)

    # remove 'edgar/data/' from address
    file$address <- gsub('edgar/data/', '', file$address)

    # subset to after min.date, before max.date
    file <- file[fdate >= min.date & fdate <= max.date, ]

    # return
    return(file)
}

##########

##### FUNCTION TO DOWNLOAD 13F FILINGS #####

# function to download 13Fs for a single CIK
download.one.cik <- function(table, base.url = 'https://www.sec.gov/Archives/edgar/data', 
                             output.folder, overwrite = FALSE, sleep = 5) {

    # first, create main folder
    folder <- paste(output.folder, unique(table$cik), sep = '/')
    if(!dir.exists(folder)) dir.create(folder)

    # if overwrite = TRUE, replace all files
    if(overwrite == TRUE) {
        files.to.download <- table$address
    } else {
        files.to.download <- table$address[!file.exists(paste(output.folder, table$address, sep = '/'))]
    }

    if(length(files.to.download) != 0) {
        # download all files
        for(i in 1:length(files.to.download)) {

            # pause for a random number of seconds between 1 and sleep to prevent overloading edgar
            if(sleep >= 1) Sys.sleep(sample(1:sleep, 1))

            # get file
            txt <- readLines(paste(base.url, files.to.download[i], sep = '/'))

            # save
            writeLines(txt, paste(output.folder, files.to.download[i], sep = '/'))
        }        
    }
}

##########

##### FUNCTIONS TO EXTRACT INFO FROM 13F FILINGS #####

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
    accession <- gsub('ACCESSION NUMBER:', '', gsub('\\t', '', txt[grepl('ACCESSION NUMBER:', txt)]))[1]
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

    # add accession, form, rdate, fdate
    biographical.txt$accession <- accession
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

# function to extract and detect type of information table
detect.infotable <- function(txt, rdate, cusips, start.rdate = 19990000) {

    # set table to NULL, type to none
    table <- NULL

    # if rdate is before 1999 output character(0)
    if(as.numeric(substr(rdate, 1, 4)) < 1999) return(character(0))

    # XML format contains <informationTable or :informationTable
    if(any(grepl('<informationTable|:informationTable', txt))) {

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

    }

    # return table, type
    return(table) 
}

# function to extract info from one 13f
extract.one.13f <- function(row, input.folder, output.folder,, overwrite = FALSE) {

    # get file
    txt <- readLines(paste(input.folder, row$address, sep = '/'))

    # get biographical data
    biographical <- get.biographical(txt, row)

    # initialize table to NULL
    table <- NULL

    # if raw table does not exist, parse and create it
    if(overwrite | !file.exists(paste(output.folder, row$address, sep = '/'))) {
        # detect infotable and its type
        table <- detect.infotable(txt, biographical$rdate[1], cusips = cusip6_universe)

        # save table
        folder <- paste(output.folder, row$cik, sep = '/')
        if(!dir.exists(folder)) dir.create(folder)
        writeLines(table, paste(output.folder, row$address, sep = '/'))
    }


    return(list(biographical = biographical, table = table))
}

##########

##### FUNCTIONS TO PARSE TABLES #####

# function to determine table type
determine.type <- function(table, numseps = 3, minspace = 3, min.frac = 0.5) {
    # easy to identify XMLs and no table
    if(length(table) == 0) {
        type <- 'none'
    } else if(any(grepl('<informationTable|:informationTable', table))) {
        type <- 'xml'
    } else {
        # get the number of tab, comma, and (multiple) space separators (approximation)
        tabseps <- sum(unlist(lapply(strsplit(table, '\t'), function(x) length(x) - 1)) >= numseps) / length(table)
        comseps <- sum(unlist(lapply(strsplit(table, '\\,'), function(x) length(x) - 1)) >= numseps) / length(table)
        spaceseps <- sum(unlist(lapply(gregexpr('[[:alnum:]]\\s+', table), 
                                       function(x) max(attr(x, 'match.length')))) >= minspace) / length(table)

        # if comseps > min.frac, set to csv; if spaceseps > min.frac, set to fwf; if tabseps > min.frac, set to tab;
        # csv ends up being a catchall for weird tables--that is ok
        type <- 'fwf'
        if(comseps >= min.frac & !is.na(comseps)) type <- 'csv'
        if(spaceseps >= min.frac & !is.na(spaceseps)) type <- 'fwf'
        if(tabseps >= min.frac & !is.na(tabseps)) type <- 'tab'
    }

    # return type
    return(type)
}

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

# function to determine which col is shares and which is value using crsp data
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

# function to parse any given table
parse.one.table <- function(extract_output, cusip.universe.all, crsp.universe.all, output.folder, error.file, 
                            overwrite = FALSE) {

    # require XML
    require(XML)

    # get biographical output
    biographical <- extract_output$biographical
    table <- extract_output$table
    type <- extract_output$tabletype

    # subset cusip.universe.all, crsp.universe.all to relevant rdates
    cusip.universe <- cusip.universe.all[[paste0('y', substr(biographical$rdate[1], 1, 4), 'q', 
                                                 ceiling(as.numeric(substr(biographical$rdate[1], 5, 6)) / 3))]]
    crsp.universe <- crsp.universe.all[yearqtr == as.yearqtr(as.Date(as.character(biographical$rdate), 
                                                                     format = '%Y%m%d'))]

    # if cleaned table does not exist, create it
    if(overwrite | !file.exists(paste(output.folder, gsub('.txt', '.csv', biographical$address), sep = '/'))) {
        
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

        # save table (create directory if it does not exist)
        if(!dir.exists(paste(output.folder, biographical$cik_row[1], sep = '/'))) {
            dir.create(paste(output.folder, biographical$cik_row[1], sep = '/'))
        }
        fwrite(out$value, paste(output.folder, gsub('.txt', '.csv', biographical$address), sep = '/'))

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

        # return table
        return(out$value)
    }
}

##########
