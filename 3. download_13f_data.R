###########################################
### 3. download_13f_data.R              ###
### Author: Morad Elsaify               ###
### Date created: 02/04/19              ###
### Date modified: 03/26/19             ###
###########################################

###########################################################################################################
### This file downloads all form 13F-HR, 13F-HR/A, 13F-NT, and 13F-NT/A since EDGAR began (1999). These ###
### filings are later parsed in extract_13f_data.R.                                                     ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/EDGAR Parsing/Code/3. download_13f_data.R', echo = TRUE)

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
library(XML)
library(parallel)

# source functions
source('/hpc/group/fuqua/mie4/EDGAR Parsing/Code/Functions/functions_general.R')
source('/hpc/group/fuqua/mie4/EDGAR Parsing/Code/Functions/functions_download.R')

# set directory
setwd('/hpc/group/fuqua/mie4/EDGAR Parsing/Data')

##### DOWNLOAD ALL 13F REPORTS #####

# get file locations
addresses <- get.all.addresses(folder = 'Master Files/All_13F', min.date = as.Date('1980-01-01'))

# download 13Fs
download.all.13f(addresses, start = 1, max.date = as.Date('1980-01-01'))

##########
