###########################################
### functions_cusip_universe.R          ###
### Author: Morad Elsaify               ###
### Date created: 02/29/20              ###
### Date modified: 03/31/20             ###
###########################################

###########################################################################################################
### This file contains functions to gather the set of CUSIP's in the SEC 13F filing universe by         ###
### quarter. The functions are used by get_cusip_universe.R.                                            ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/functions_cusip_universe.R', echo = TRUE)

##### FUNCTION TO ENSURE ACCURACY (I.E., MAKE SURE CHECKSUM IS CORRECT) #####

checksum.value <- function(x, pos) {

    # convert x to numeric if numeric, else do 9 + LETTERS
    x <- ifelse(grepl('[[:digit:]]', x), as.numeric(x), which(x == LETTERS) + 9)

    # multiply by 2 if in an even position
    x <- ifelse(pos %% 2 == 0, 2*x, x)

    # take floor and mod 10
    x <- floor(x / 10) + (x %% 10)

    # return x
    return(x)
}


checksum.check <- function(cusip, return.check = TRUE) {

    # first, remove any spaces from cusip
    cusip <- gsub(' ', '', cusip)

    # split cusip into letters
    cusip_split <- strsplit(cusip, '')

    # Reduce cusip_split[1:8] on checksum.value
    sums <- lapply(cusip_split, function(cusip) Reduce(function(sum, x) sum + checksum.value(cusip[x], x), 1:8, init = 0))

    # get value
    value <- lapply(sums, function(x) (10 - (x %% 10)) %% 10)

    # check if correct
    check <- lapply(1:length(value), function(x) cusip_split[[x]][9] == value[[x]])
    check <- lapply(check, function(x) ifelse(is.na(x), FALSE, x))

    # return value
    if(return.check) {
        return(unlist(check))
    } else {
        return(unlist(value))
    }    
}

##########

##### FUNCTIONS TO LOAD A PDF AND PARSE THE PAGES INTO A LIST OF TABLES #####

# function to separate a fixed width table based on supplied seps
separate <- function(seps) {
    function(row) {
        sapply(1:(length(seps) - 1), function(x) substr(row, seps[x] + 1, seps[x+1]))
    }
}

# function to parse an individual page
parse.one.page <- function(page, seps) {

    # require readr
    require(readr)

    # split by '\n'
    page <- strsplit(page, '\n')[[1]]

    # search for "CUSIP NO"
    if(any(grepl('CUSIP NO', page))) {
        # remove all rows above CUSIP NO
        page <- page[min(which(grepl('CUSIP NO', page))):length(page)]

        # automatically detect seps if missing
        if(missing(seps)) {
            # start at 0
            seps <- c(0)

            # manually form seps
            seps <- c(seps, gregexpr('ISSUER NAME', page[[1]])[[1]] - 1)
            seps <- c(seps, gregexpr('ISSUER DESCRIPTION', page[[1]])[[1]] - 1)
            seps <- c(seps, gregexpr('STATUS', page[[1]])[[1]] - 1)
            seps <- c(seps, max(nchar(page)))
        } else {
            seps <- c(seps, max(nchar(page)))
        }

        # convert page to table
        rows <- lapply(page, separate(seps))
        rows <- lapply(rows, trimws)
        table <- do.call(rbind, rows)

        # if nrow > 2, return table
        if(nrow(table) > 2) return(table)
    }
}

# function to convert all pages of a pdf into tables
load.one.pdf <- function(url, type = c('plain', 'ocr'), ...) {

    # require pdftools, tesseract
    require(pdftools)
    require(tesseract)

    # match type arg
    type <- match.arg(type, type, several.ok = FALSE) 

    # load url (may take a very long time if type == 'ocr')
    if(type == 'plain') {
        pdf <- pdf_text(url)
    } else {
        pdf <- pdf_ocr_text(url)
    }

    # parse the pdf by page
    pdfs <- lapply(pdf, parse.one.page, ...)

    # print progress
    cat('Done url:', url, '! \n')

    return(pdfs)
}

##########

##### FUNCTION TO COMBINE PAGES INTO A SINGLE CLEAN TABLE #####

# function to combine and format tables
clean.tables.post <- function(tablist, colnames, cusipcol = 'cusipno') {

    # drop nulls
    tablist <- tablist[-which(sapply(tablist, is.null))]

    # convert each to a table, add colnames
    convert.to.dt <- function(x, colnames) {
        dt <- as.data.table(x[2:nrow(x), ])
        colnames(dt) <- colnames
        return(dt)
    }
    if(missing(colnames)) {
        tablist <- lapply(tablist, function(x) convert.to.dt(x, tolower(gsub('\\.', '', gsub(' ', '', x[1, ])))))
    } else {
        tablist <- lapply(tablist, function(x) convert.to.dt(x, colnames))
    }

    # rbind all tables
    table <- rbindlist(tablist)

    # remove non-uniques, those with missing cusips
    table <- unique(table[get(cusipcol, table) != '', ])

    # create asterisk column
    table$asterisk <- ifelse(grepl('\\*', get(cusipcol, table)), '*', '')
    table[, (cusipcol) := gsub('[^[:alnum:]]', '', (gsub('\\*', '', get(cusipcol, table))))]
    
    # check values
    table$check <- checksum.check(get(cusipcol, table))

    # return table
    return(table)
}

# function to clean cusipnos
clean.cusipno <- function(table) {
    # remove whitespace, convert to upper
    cusips <- trimws(toupper(table$cusipno))
    others <- trimws(table$other)

    # make list of issues, their replacements, and other errors
    issues <-  c('4%', '%', '\\.', '^\\|', '\\|', '8\\&$', '\\&', 'J/', '/ ', '/', ')', 'G@|4@|@', ' 1] ', '1]|]j|]', '¥Y|¥', '§', '\\?', '\\$', '}$', '}')
    replace <- c(''  , '4', ''   , ''    , '1'  , ''     , '8'  , 'J' , 'J' , '' , 'J', '4'      , '11'  , '1'      , 'Y'   , '4', '7'  , '3'  , '1' , '')
    errors <- c(';', "'", '\\+', '°', '=', ',', '\\(', '~', '¢', '\\_', '\\:', '\\#', '\\.', '-', '—')

    # fix all errrors
    for(i in 1:length(errors)) {
        cusips <- gsub(errors[i], '', cusips)
    }

    # first, do not change the cusips that are validated
    check <- checksum.check(cusips)

    # for those that do not check, correct each issue iteratively, also trying to remove alphanumeric/nonspace characters
    for(i in 1:length(issues)) {
        # check replacing each issue, removing non alphanumeric
        candidate1 <- trimws(gsub(issues[i], replace[i], cusips[check != TRUE]))
        candidate2 <- trimws(gsub('[^[:alnum:][:space:]]', '', cusips[check != TRUE]))

        # see if either changes a FALSE to a TRUE
        candidate1_check <- checksum.check(candidate1)
        candidate2_check <- checksum.check(candidate2)

        # combine, default to candidate1
        combined <- candidate1
        combined_check <- candidate1_check
        inds <- candidate2_check == TRUE & !is.na(candidate2_check)
        combined[inds] <- candidate2[inds]
        combined_check[inds] <- candidate2_check[inds]
        
        # replace the cusips
        cusips[check != TRUE] <- combined
        check[check != TRUE] <- combined_check
    }

    # remove all spaces
    cusips <- gsub('[^[:alnum:]]', '', cusips)
    
    # expand the cusipno to be 9 digits if too short
    short <- nchar(cusips) < 9
    cusips[short] <- paste0(cusips[short], substr(trimws(gsub('[^[:alnum:]]', '', others[short])), 1, 
                                                  9 - nchar(cusips[short])))

    # shorten cusips to be first 9 alphanumeric characters
    long <- nchar(cusips) > 9
    cusips[long] <- substr(cusips[long], 1, 9)

    # add columns to table
    table$cusipno <- cusips
    table$other <- others

    # drop rows with all letters
    table <- table[nchar(trimws(gsub('[^0-9]', '', cusipno))) > 0, ]

    # return table
    return(table)
}

# function to separate issuer name vs. issuer description
separate.name.description <- function(table) {
    # remove punctuation at end of other
    table$other <- toupper(trimws(gsub('[[:punct:]]$', '', trimws(table$other))))

    # initialize issuerdescription, issuername
    table$issuername <- table$other
    table$issuerdescription <- ''
    
    # first, separate by common issuer description endings
    common.descriptions <- c('COM', 'PUT', 'CALL', 'ORD', 'CLA')
    for(i in 1:length(common.descriptions)) {
        table$issuername <- trimws(gsub(paste0('\\b', common.descriptions[i], '$'), '', table$issuername))
        table$issuerdescription <- ifelse(grepl(paste0('\\b', common.descriptions[i], '$'), table$other), 
                                          common.descriptions[i], 
                                          table$issuerdescription)
    }

    # next, get unique description starts
    unique.description.starts <- c('COM PAR', 'SPONSORED ADR', 'SPONS ADR', 'SPON ADR', 'CL A', 'CL B', 'COM NEW', 
                                   'WT EXP', 'DEB CONV', 'DEB CV', 'SUB NT CV', 'SB NT CV', 'WT B EX', 'PFD B CON', 
                                   'SUB NT C', 'ADR B', 'COM A', 'COM B', 'SBNTC', 'CDT-', 'NEW UNIT EX', 'CALL A', 
                                   'PUT RE', 'WT A EX', 'ORD NEW', 'SB DB CV', 'WT C EX', 'UNIT EX', 'PFD CV B', 
                                   'COM ZD', 'TR UNIT', 'SD CV Z')
    for(i in 1:length(unique.description.starts)) {
        table$issuername <- trimws(gsub(paste0('\\b', unique.description.starts[i]), '', table$issuername))
        table$issuerdescription <- ifelse(grepl(paste0('\\b', unique.description.starts[i]), table$other), 
                                          unique.description.starts[i], 
                                          table$issuerdescription)
    }

    # return table
    return(table)
}

# function to clean archive data
clean.tables.archive <- function(tablist) {
    
    # drop nulls
    tablist <- tablist[-which(sapply(tablist, is.null))]

    # drop the first row, remove all empty rows, convert to data table, add colnames
    convert.to.dt <- function(x) {
        # drop empty cusips
        x <- x[x[, 1] != '', ]

        # make dt, add colnames
        dt <- as.data.table(x[2:nrow(x), ])
        colnames(dt) <- c('cusipno', 'other')

        # return dt
        return(dt)
    }
    tablist <- lapply(tablist, convert.to.dt)

    # rbind all tables
    table <- rbindlist(tablist)

    # remove cusipnos that have the word "TOTAL", nchar >= 10 letters
    table <- table[!grepl('total', cusipno, ignore.case = TRUE) & nchar(cusipno) >= 10, ]

    # now, clean up cusips
    table <- clean.cusipno(table)

    # first, remove punctuation at end of other
    table$other <- toupper(trimws(gsub('[[:punct:]]$', '', trimws(table$other))))

    # remove everything after ADDED or DELETED, move to status flag
    statuslocation <- unlist(lapply(gregexpr('ADDED|DELETED', table$other), function(x) rev(x)[[1]]))
    table$status <- ifelse(statuslocation > 0, substr(table$other, statuslocation, nchar(table$other)), '')
    table$status <- gsub('[^[:alpha:]]', '', table$status)
    table$status <- ifelse(grepl('ADDED', table$status), 'ADDED', 'DELETED')
    table$other <- gsub('ADDED.*$|DELETED.*$', '', table$other)

    # get asterisk flag
    asterisk.symbols <- c('*', '@', '%', '®', '=', '‘', '®*', '%*')
    table$asterisk <- ifelse(trimws(substr(table$other, 1, 1)) %in% asterisk.symbols |
                             trimws(substr(table$other, 1, 2)) %in% asterisk.symbols, '*', '')
    for(i in 1:length(asterisk.symbols)) {
        table$other <- trimws(gsub(paste0('^\\', asterisk.symbols[i]), '', table$other))
    }

    # separate issuer name vs. issuer description
    table <- separate.name.description(table)

    # finally check cusipnos
    table$check <- checksum.check(table$cusipno)

    # get columns
    table <- table[, c('cusipno', 'issuername', 'issuerdescription', 'status', 'asterisk', 'check')]

    # return table
    return(table)
}

##########
