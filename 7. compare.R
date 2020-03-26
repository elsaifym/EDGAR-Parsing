###########################################
### 7. compare.R                        ###
### Author: Morad Elsaify               ###
### Date created: 03/20/20              ###
### Date modified: 03/21/20             ###
###########################################

###########################################################################################################
### This file compares the data scraped from EDGAR in previous files with two external sources of 13F   ###
### data. The first comes from Thomson Reuters (the most common source of 13F data, but has several     ###
### data issues). The second comes from Michael Sinkinson, who scraped EDGAR to get around the issues   ###
### with the Thomson Reuters data. This data is available from his website:                             ###
### https://sites.google.com/view/msinkinson/research/common-ownership-data.                            ###
###########################################################################################################

# source('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Code/7. compare.R', echo = TRUE)

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

# set directory
setwd('~/Dropbox/Mory/Duke/Research/Data Projects/EDGAR Parsing/Data')

# load data
holdings <- fread('Final Tables/holdings.csv')
tr <- fread('Other Holdings/tr_holdings.csv')
sinkinson <- fread('Other Holdings/sinkinson_holdings.csv')
crspq <- fread('crspq.csv')
