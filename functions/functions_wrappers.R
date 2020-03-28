###########################################
### functions_wrappers.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/28/20              ###
### Date modified: 03/28/20             ###
###########################################

###########################################################################################################
### This file contains all wrappers for the functions used to download, extract, and parse 13F filings. ### 
### This should be used when users either want to gather 13F data for more than one CIK or to           ###
### parallelize within R (using mclapply). Due to the scale of the 13F filings, parallelizing outside   ###
### R is recommended.                                                                                   ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/functions/functions_wrappers.R', echo = TRUE)

##### GENERIC WRAPPER TO SPLIT JOBS INTO SETS OF n EACH TO AVOID OVERLOADING MEMORY #####

wrapper.split <- function(obj.to.split, n, FUN, ...) {
    # get length of obj.to.split
    len <- length(obj.to.split)

    # initialize output
    out <- as.list(rep(NA, len))

    # iterate
    for(i in 1:ceiling(len / n)) {
        # run for that set
        temp <- FUN(obj.to.split[(1 + (i-1)*n):min((i*n), len)], ...)

        # append in right position to out
        out[(1 + (i-1)*n):min((i*n), len)] <- temp

        # print progress
        cat(paste('Done', i, 'of', ceiling(len / n)), 'sets! \n')
    }

    return(out)
}

##########

##### WRAPPER TO LAPPLY ADDRESS GATHERING #####

get.all.addresses <- function(folder = 'master_files/all_13f', num.cores = detectCores(), ...) {
    # get every file in directory
    file.names <- dir(folder, pattern = '.csv')

    # get start.time
    start.time <- Sys.time()

    # lapply over file.names
    if(num.cores == 1) {
        addresses <- lapply(1:length(file.names), 
                            function(i) get.one.address(file.name = file.names[i], folder = folder, counter = i, 
                                                        total = length(file.names), start.time = start.time, ...))
    } else {
        # require parallelization
        require(parallel)

        addresses <- mclapply(1:length(file.names), 
                              function(i) get.one.address(file.name = file.names[i], folder = folder, counter = i, 
                                                          total = length(file.names), start.time = start.time, ...), 
                              mc.cores = num.cores)
    }

    # return rbinded list of addresses
    return(addresses)
}

##########

##### WRAPPER TO DOWNLOAD ALL FILINGS #####

# wrapper to lapply over all 13Fs
download.all.cik <- function(tables, output.folder, overwrite, num.cores = detectCores(), ...) {

    # get start.time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        results <- lapply(1:length(tables), 
                          function(i) download.one.cik(tables[[i]], output.folder = output.folder, counter = i, 
                                                       total = length(tables), start.time = start.time, ...))
    } else {
        # require parallelization
        require(parallel)

        results <- mclapply(1:length(tables), 
                            function(i) download.one.cik(tables[[i]], output.folder = output.folder, counter = i, 
                                                         total = length(tables), start.time = start.time, ...), 
                            mc.cores = num.cores)
    }
}

##########

##### WRAPPER TO EXTRACT ALL 13F BIOGRAPHICAL DATA AND INFORMATION TABLE #####

# function to extract info fromall 13fs
extract.all.13f <- function(addresses, cusips, input.folder, output.folder, overwrite, num.cores = detectCores(), ...) {

    # get start time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        results <- lapply(1:nrow(addresses), 
                          function(i) extract.one.13f(row = addresses[i, ], cusips = cusips, input.folder = input.folder, 
                                                      output.folder = output.folder, overwrite = overwrite, 
                                                      counter = i, total = nrow(addresses), start.time = start.time))
    } else {
        # require parallelization
        require(parallel)

        results <- mclapply(1:nrow(addresses), 
                            function(i) extract.one.13f(row = addresses[i, ], cusips = cusips, input.folder = input.folder, 
                                                        output.folder = output.folder, overwrite = overwrite, 
                                                        counter = i, total = nrow(addresses), start.time = start.time),
                            mc.cores = num.cores)
    }

    # return results
    return(results)
}

##########

##### WRAPPER TO PARSE 13F TABLES #####

parse.all.tables <- function(tablist, cusip.universe.all, crsp.universe.all, output.folder, error.file, overwrite, 
                             num.cores = detectCores(), ...) {

    # require XML package
    require(XML)

    # get start time
    start.time <- Sys.time()

    # lapply over addresses
    if(num.cores == 1) {
        tables <- lapply(1:length(tablist), 
                         function(i) parse.one.table(extract_output = tablist[[i]], 
                                                     cusip.universe.all = cusip.universe.all, 
                                                     crsp.universe.all = crsp.universe.all, output.folder = output.folder, 
                                                     error.file = error.file, overwrite = overwrite, 
                                                     counter = i, total = length(tablist), start.time = start.time))
    } else {
        # require parallelization
        require(parallel)

        tables <- mclapply(1:length(tablist), 
                           function(i) parse.one.table(extract_output = tablist[[i]], 
                                                       cusip.universe.all = cusip.universe.all, 
                                                       crsp.universe.all = crsp.universe.all, output.folder = output.folder, 
                                                       error.file = error.file, overwrite = overwrite, 
                                                       counter = i, total = length(tablist), start.time = start.time), 
                           mc.cores = num.cores)
    }

    # return tables
    return(tables)
}

##########