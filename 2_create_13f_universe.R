###########################################
### 2_get_13f_universe.R                ###
### Author: Morad Elsaify               ###
### Date created: 03/27/20              ###
### Date modified: 03/27/20             ###
###########################################

###########################################################################################################
### This file creates a file for each 13F-HR/13F-HR/A/13F-NT/13F-NT/A reporting institution. Each       ###
### master file (downloaded using 1_get_master_files.R) is iterated through and the rows containing a   ###
### 13F report are appended to the file with the appropriate CIK number. Each file is in the            ###
### subdirectory 'all_13f' with each reporting CIK being given its own file with that name. Only filers ###
### of 13F-HR and 13F-HR/A are included in the subdirectory 'only_13f_hr'.                              ###
###########################################################################################################

# source('/hpc/group/fuqua/mie4/edgar_parsing/code/2_get_13f_universe.R', echo = TRUE)

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
setwd('/hpc/group/fuqua/mie4/edgar_parsing/data/master_files')

# set output folders
all_13f_folder <- 'all_13f'
only_hr_folder <- 'only_13f_hr'

# delete those folders, create them again (to prevent duplicates)
unlink(all_13f_folder, recursive = TRUE)
unlink(only_hr_folder, recursive = TRUE)
if(!dir.exists(all_13f_folder)) dir.create(all_13f_folder)
if(!dir.exists(only_hr_folder)) dir.create(only_hr_folder)

# get list of master files
master_files <- list.files(include.dirs = FALSE, pattern = 'master')

##### CREATE 13F UNIVERSE FOR EACH CIK APPEARING IN MASTER FILES #####

# iterate over these files
for(i in 1:length(master_files)) {

    # load file
    temp <- fread(master_files[i], skip = 11, sep = '|', header = FALSE, 
                  col.names = c('cik', 'company_name', 'form', 'rdate', 'filename'))

    # subset to those with 13Fs and those with HRs
    all_13f <- temp[form %in% c('13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A'), ]
    only_hr <- temp[form %in% c('13F-HR', '13F-HR/A'), ]

    # iterate over rows in all_13f, add rows to file
    for(j in 1:nrow(all_13f)) {
        write.table(all_13f[j, ], file = paste0(all_13f_folder, '/', all_13f[j, ]$cik, '.csv'), append = TRUE, 
                    row.names = FALSE, col.names = FALSE, sep = ',')
    }

    # iterate over rows in only_hr, add rows to file
    for(j in 1:nrow(only_hr)) {
        write.table(only_hr[j, ], file = paste0(only_hr_folder, '/', only_hr[j, ]$cik, '.csv'), append = TRUE, 
                    row.names = FALSE, col.names = FALSE, sep = ',')
    }

    # print progress
    cat('Done', i, 'of', length(master_files), '\r')
}

##########
