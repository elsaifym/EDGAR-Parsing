###########################################
### get_data.R                          ###
### Author: Morad Elsaify               ###
### Date created: 03/12/20              ###
### Date modified: 03/27/20             ###
###########################################

###########################################################################################################
### This file gathers quarterly CRSP data used to validate observations in 13F filings.                 ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/get_data.R', echo = TRUE)

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
library(RPostgres)

# set directory
setwd('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/data')

# establish wrds connection
establish.connection <- function(user, password, host = 'wrds-pgdata.wharton.upenn.edu', port = 9737, 
                                 sslmode = 'require', dbname = 'wrds') {
    # require username/password
    if(missing(user)) {
        user <- readline('Enter your WRDS username: ')
    }
    if(missing(password)) {
        password <- readline('Enter your WRDS password: ')
    }

    # establish connection
    wrds <- dbConnect(Postgres(), host = host, port = port, user = user, password = password,
                      sslmode = sslmode, dbname = dbname)

    return(wrds)
}
wrds <- establish.connection()

# function to get data from query
get.data <- function(call, connection = wrds, ...) {
    # require data.table
    require(data.table)

    # get data
    res <- dbSendQuery(wrds, call)
    data <- as.data.table(dbFetch(res, ...))

    # clear query
    dbClearResult(res)

    # return data
    return(data)
}

# get start, end dates
start_ <- '1990-01-01'
end_ <- Sys.Date()

add.dates <- function(x, datevar, type = c('both', 'start', 'end'), start = start_, end = end_, and) {
    # match type arg
    type <- match.arg(type, type, several.ok = FALSE)

    # append single quotes ' to start and end dates
    startquotes <- paste0("'", start, "'")
    endquotes <- paste0("'", end, "'")

    # make datestring based on type
    if(type == 'both') {
        datestring <- paste("where", datevar, "between", startquotes, "and", endquotes)
    } else if(type == 'start') {
        datestring <- paste("where", datevar, ">=", startquotes)
    } else if(type == 'end') {
        datestring <- paste("where", datevar, "<=", endquotes)
    }

    # if and not missing, paste on as well
    if(!missing(and)) datestring <- paste(datestring, 'and', and)

    # return x and datestring pasted together
    return(paste(x, datestring))
}

##### GET CRSP DATA #####

# get info from monthly stock file
msf_call <- add.dates("select permno, date, prc, shrout, cfacpr, cfacshr from crsp.msf", 'date')
msf <- get.data(msf_call)

# get info from daily event file
mse_call <- add.dates("select permno, date, ncusip from crsp.mseall", 'date')
mse <- unique(get.data(mse_call))

# make yearmon variable
msf[, yearmon := as.yearmon(date)]

# function to merge events and stock file
merge.events.stock <- function(events, stocks, idvar = 'permno', by = c('permno', 'date'), ...) {
    # create event, stock flags
    events[, event := TRUE]
    stocks[, stock := TRUE]

    # only keep variables in events not in stocks plus by vars
    varlist <- unique(c(idvar, by, setdiff(colnames(events), colnames(stocks))))
    events <- events[, ..varlist]

    # merge, fill event flag
    merged <- merge(stocks, events, by = by, all.x = TRUE, ...)
    merged[is.na(event), event := FALSE]   
    merged[is.na(stock), stock := FALSE]   

    # set list of variables to carry forward
    carry <- c('comnam', 'exchcd', 'hexcd', 'hsiccd', 'hsicig', 'hsicmg', 'issuno', 'naics', 'ncusip', 'permco', 'permno', 
               'primexch', 'secstat', 'shrcd', 'shrcls', 'siccd', 'ticker', 'trdstat', 'tsymbol', 'shrflg', 'shrout', 
               'mmcnt', 'nmsind', 'nsdinx', 'facshr', 'facpr')

    # get set of carry variables in events
    carry <- intersect(carry, colnames(events))

    # sort by by vars
    merged <- merged[order(get(by, merged)), ]

    # make first indicator
    merged[, first := seq_len(.N) == 1, by = idvar]
    
    # if first, fill backwards
    fill.backwards <- function(x, first) ifelse(first == TRUE, na.locf(x, fromLast = TRUE, na.rm = FALSE), x)
    merged[, (carry) := lapply(.SD[, -'first'], fill.backwards, first = first), .SDcols = c(carry, 'first'), by = idvar]

    # if not an event, fill forwards
    fill.forwards <- function(x, event)  ifelse(event == FALSE, na.locf(x, na.rm = FALSE), x)
    merged[, (carry) := lapply(.SD[, -'first'], fill.forwards, event = event), .SDcols = c(carry, 'first'), by = idvar]

    # drop vars and return
    merged <- merged[, !(colnames(merged) %in% c('stock', 'event', 'first', 'year', 'month')), with = FALSE]
    return(merged)
}

# merge mse, msf
crsp <- merge.events.stock(events = mse, stocks = msf)

# make yearqtr variable
crsp <- crsp[order(permno, date), ]
crsp[, yearqtr := as.yearqtr(date)]

# fill prices, shrout forward
crsp[, `:=`(prc = na.locf(prc, na.rm = FALSE), shrout = na.locf(shrout, na.rm = FALSE)), by = permno]

# get last value in quarter
crspq <- crsp[, .SD[.N, ], by = list(permno, yearqtr)]

# take absolute value of price, determine stock splits
crspq[, `:=`(prc = abs(prc), split = cfacpr != shift(cfacpr, type = 'lag', 1)), by = permno]

# only keep ncusip, yearqtr, prc, shrout, split
crspq <- crspq[, c('permno', 'ncusip', 'yearqtr', 'prc', 'shrout', 'split')]

##########

# save data, remove
fwrite(crspq, 'crspq.csv')
