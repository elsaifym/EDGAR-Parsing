# EDGAR-Parsing

This repository contains all the code necessary to download, extract, and parse 13F filings on EDGAR.

The files should be run in the sequence described below. Ensure that all directories are accurate and all packages are installed before running.

## Download External Data

get_data.R: Download external price data from CRSP. Note: a valid WRDS subscription is necessary. This is used to determine columns and validate the parsed information.

get_cusip_universe.R: Read and process the list of official [13(f) securities](https://www.sec.gov/divisions/investment/13flists.htm) comes from the SEC. This is used to extract the relevant rows from the information table.

## Download 13F Filings

1_get_master_files.R: Download files that contain a list of all SEC filings by quarter, including the filer CIK, form type, and form location in the EDGAR database.

2_create_13f_universe.R: Creates a file for each CIK that files a Form 13F. These files include the form filed, report date, and form location in the EDGAR database.

## Process 13F filings

There are two options to processing the 13F filings (extract biographical information and parse the information table). The first is using the task manager SLURM for each CIK (recommended). The second is to parallelize the process within a single R session.

To use SLURM, run submit_job.sh. This utilizes 3_process_all_ciks.R. Note: when doing this, ensure the array size is equal to the number of files in the master_files/all_13f folder.

To use in a single R session, run 3_process_one_cik.R.

There are several options to more quickly update the database:
1. overwrite_download (defaults to FALSE) will re-download all 13F filings if set to TRUE. Otherwise, only filings not in the working directory will be downloaded.
2. overwrite_extract (defaults to FALSE) will re-extract biographical information and information tables (in plain text) if set to TRUE. Otherwise, will only extract this information for filings for which this has not been done.
3. overwrite_parse (defaults to FALSE) will re-parse the information table into csv if set to TRUE. Otherwise, will only parse this information for filings for which this has not been done.

## Combine Tables

4_combine_tables.R aggregates the data. This includes compiling and formatting the biographical data of the institutional investment managers, as well as creating the holdings data. Raw holdings data is created simply by appending all parsed information tables together. A final version of the holdings data is created by dropping options and callable bonds, extraneous fields, and applying several filters to the data, including keeping only accurately-parsed tables.

## Functions

The "functions" folder contains the suite of functions used in the process. See these files individually for a description of the functions contained therein.
