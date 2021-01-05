###########################################
### get_cusip_universe.R                ###
### Author: Morad Elsaify               ###
### Date created: 02/27/20              ###
### Date modified: 03/27/20             ###
###########################################

###########################################################################################################
### This file compiles a list of eligible 13F securities compiled by the SEC. These SEC compilations    ###
### are in PDF format and, prior to 2003, are are OCR-scanned compies, which complicates the process.   ###
### The pdfs are located at https://www.sec.gov/divisions/investment/13flists.htm.                      ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/get_cusip_universe.R', echo = TRUE)

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
library(pdftools)
library(tesseract)
library(parallel)

# source functions
source('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/code/functions/functions_cusip_universe.R')

# set directory
setwd('/hpc/group/fuqua/mie4/data_projects/edgar_parsing/data')

# load crsp
crspq <- fread('crspq.csv')

# make yearqtr a yearqtr variable
crspq[, yearqtr := as.yearqtr(yearqtr)]

# add check digit to crspq cusip, make cusip6 variable
crspq$cusip9 <- checksum.check(crspq$ncusip, return.check = FALSE)
crspq$cusip9 <- paste0(crspq$ncusip, crspq$cusip9)
crspq$cusip6 <- substr(crspq$ncusip, 1, 6)

##### APPLY THIS TO YEARS 2004-2019 #####

# make set of urls
years <- 2004:year(Sys.Date())
q <- 1:4
names <- sort(c(outer(paste0('y', years, 'q'), q, FUN = 'paste0')))
urls <- sort(c(outer(paste0(years, 'q'), paste0(q, '.pdf'), FUN = 'paste0')))
urls <- paste0('https://www.sec.gov/divisions/investment/13f/13flist', urls)
urls[1] <- 'https://www.sec.gov/divisions/investment/13f-list.pdf'

# load.one.pdf over urls
post <- mclapply(urls, load.one.pdf, mc.cores = detectCores())
names(post) <- names

# clean post tables
post <- mclapply(post, clean.tables.post, mc.cores = detectCores())

##########

##### APPLY TO YEARS 1999-2003 #####

# make set of urls for archive
years <- 1999:2003
q <- 1:4
archive.names <- sort(c(outer(paste0('y', years, 'q'), q, FUN = 'paste0')))
archive.urls <- sort(c(outer(paste0(years, 'q'), paste0(q, '.pdf'), FUN = 'paste0')))
archive.urls <- paste0('https://www.sec.gov/divisions/investment/13f/13flist', archive.urls)

# load.one.pdf over urls
archive <- mclapply(archive.urls, load.one.pdf, type = 'ocr', seps = c(0, 12), mc.cores = detectCores())
names(archive) <- archive.names

# clean archive tables
archive <- mclapply(archive, clean.tables.archive, mc.cores = detectCores())

##########

##### FILL BACKWARDS BASED ON "ADDED" FLAG #####

# first, combine archive and post
combined <- c(archive, post)

# now, save combined
load(archive, file = 'cusip_universe/cusip_universe_raw_pre2004.Rdata')
load(post, file = 'cusip_universe/cusip_universe_raw_post2004.Rdata')
load(combined, file = 'cusip_universe/cusip_universe_raw.Rdata')

# remove all cusips less than 9 digits
combined <- lapply(combined, function(x) x[nchar(cusipno) == 9, ])

# iterate over length of combined, starting backwards
for(i in 2:length(combined)) {
    # get the set of those that are not added in i
    notadded <- combined[[i]][status != 'ADDED', ]

    # difference this with combined[[i-1]]
    notadded <- notadded[!(notadded$cusipno %in% combined[[i-1]]$cusipno), ]
    print(nrow(notadded))

    # rbind notadded to combined[[i-1]] sort by cusipno
    combined[[i-1]] <- rbind(combined[[i-1]], notadded)[order(cusipno), ]
}

# iterate over those in post once more
for(i in 2:length(archive)) {
    # get the set of those that are not added in i
    notadded <- combined[[i]][status != 'ADDED', ]

    # difference this with combined[[i-1]]
    notadded <- notadded[!(notadded$cusipno %in% combined[[i-1]]$cusipno), ]
    print(nrow(notadded))

    # rbind notadded to combined[[i-1]] sort by cusipno
    combined[[i-1]] <- rbind(combined[[i-1]], notadded)[order(cusipno), ]
}

# now, form cusip6, cusip8, cusip9
for(i in 1:length(combined)) {
    combined[[i]]$cusip6 <- substr(combined[[i]]$cusipno, 1, 6)
    combined[[i]]$cusip8 <- substr(combined[[i]]$cusipno, 1, 8)
    combined[[i]]$cusip9 <- substr(combined[[i]]$cusipno, 1, 9)
    combined[[i]]$cusipno <- NULL
    combined[[i]] <- combined[[i]][, c('cusip6', 'cusip8', 'cusip9', 'issuername', 'issuerdescription', 'status', 
                                       'asterisk','check')]
}

# add indicator if in CRSP
for(i in 1:length(combined)) {
    combined[[i]]$in_crsp <- combined[[i]]$cusip8 %in% crspq[yearqtr == as.yearqtr(names(combined[i]), format = 'y%Yq%q'), ]$ncusip
}

# add those not in the 13f filings but are in crsp
for(i in 1:length(combined)) {
    # get the cusips in rspq but not in 13fs for that yearqtr
    to.add <- crspq[yearqtr == as.yearqtr(names(combined[i]), format = 'y%Yq%q'), ][!ncusip %in%  combined[[i]]$cusip8, ]

    # format to.add for rbinding
    to.add[, `:=`(cusip8 = substr(cusip9, 1, 8), cusip6 = substr(cusip9, 1, 6), issuername = '', issuerdescription = '', 
                  status = '', asterisk = '', check = TRUE, in_crsp = TRUE)]
    cols.to.keep <- colnames(combined[[i]])
    to.add <- to.add[, ..cols.to.keep]
    print(nrow(to.add))

    # rbind the two, sort by cusip9
    combined[[i]] <- rbind(combined[[i]], to.add)[order(cusip9), ]
}

# now, save combined
save(combined, file = 'cusip_universe/cusip_universe.Rdata')

# finally, just save cusip6 vector, and cusip6 in crsp
cusip6_universe <- lapply(combined, function(x) unique(x$cusip6))
cusip6_crsp_universe <- lapply(combined, function(x) unique(x[in_crsp == TRUE, ]$cusip6))

# finally, save cusip6_universe
save(cusip6_universe, file = 'cusip_universe/cusip6_universe.Rdata')
save(cusip6_crsp_universe, file = 'cusip_universe/cusip6_crsp_universe.Rdata')

##########
