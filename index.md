# Form 13F Data

Form 13F data have been used widely in finance among both academic researchers and practitioners. These data are required to be submitted quarterly for all institutional investment managers overseeing at least $100 million in official 13(f) securities, which primarily include U.S. exchange-traded stocks. More information about the requirements and information contained in Form 13F can be found on the [SEC's website](https://www.sec.gov/divisions/investment/13ffaq.htm).

These data are usually gathered from third-party vendors, such as Thomson-Reuters and FactSet. I provide open-source access to this data directly from SEC's [EDGAR](https://www.sec.gov/edgar.shtml), which makes these filings publicly available as they are submitted.

This page describes the process of downloading, parsing, and compiling all 13F filings (including amended filings) on EDGAR from 1999 through the end of 2019. The repository with all code used in this process can be found on [GitHub](https://github.com/elsaifym/EDGAR-Parsing). All intermediate and final output is available on [Dropbox](https://www.dropbox.com/sh/27mxydmiume3t0e/AADyZkVZjwZe5Id9n7FbGTlea?dl=0).

# Methodology

There are six main steps to processing the data:
1. Extract the list of official [13(f) Securities](https://www.sec.gov/divisions/investment/13flists.htm) for each quarter. Prior to 2004, the lists are scanned archives, so CUSIPs are extracted using optical character recognition (OCR) to get an exhaustive list of all CUSIPs for each quarter. Because of possible errors in this process, gaps are filled in using the previous quarter's list of 13(f) securities.
2. Download all Form 13F filings. These include amended filings (13F-A), in which managers update existing filings with omitted holdings, and notices (13F-NT), in which managers state that the applicable securities appear on someone else's report.
3. Extract biographical information about the investment manager. This includes the name, central index key (CIK), address, and phone number of the institutional investment manager.
4. Extract the information table in plain text. This information table (usually) contains the security name, type of security, CUSIP, position value, and number of shares held, among other fields.
5. Parse the information table into a machine-readable format.
    * After 2013Q2, the SEC required that all filings must be submitted in XML format, which makes this step straightforward.
    * Prior to 2013Q2, there is remarkable heterogeneity in the format and content of the information tables. Some tables are formatted as fixed-width, while others are comma- or tab-separated. Others switch the ordering of the columns, omit certain columns, or provide extraneous ones (such as price). To extract the information from these tables, all rows that contained the CUSIP of an official 13(f) security were kept. The number of shares and value of the position were extracted from the table based on which column ordering resulted in the most accurate report. Report accuracy is defined as the proportion of rows where the 13F-implied price (value / number of shares) is within 10% of the price reported by the Center for Research in Security Prices ([CRSP](http://www.crsp.org)) as of that report date.
6. Combine and validate the tables. The data are appended together to generate a raw database of 13F holdings. Various validations and filters are then applied to generate a final database.
    *
    * To validate each report, the accuracy of reports are compared between XML (which are parsed with very high accuracy) and non-XML filings. The accuracy for each report is calculated as in Step 5, and all reports with accuracies below the 1% percentile of XML filings (50% report accuracy) are dropped.
    * All observations whose 13F-implied price deviates from the CRSP price by greater than the 99% percentile of XML filings (15% price deviation) are dropped.
    * All observations whose percent ownership exceeds the 99.95% percentile of XML filings (39.1% of shares outstanding) are dropped.

# Data

The [Dropbox](https://www.dropbox.com/sh/27mxydmiume3t0e/AADyZkVZjwZe5Id9n7FbGTlea?dl=0) folder contains the raw, intermediate, and final outputs, which are described below.
1. crspq.csv: This contains the external price data used to determine columns and validate the parsed tables. This is downloaded in get_data.R (a valid WRDS subscription is required).
2. cusip_universe: This folder contains the CUSIP universe used to extract rows from the 13F information tables. The extraction relies on 6-digit CUSIPs, and the relevant file is cusip6_universe.Rdata. This is created in get_cusip_universe.R.
3. master_files: This folder contains all master files since 1999. These files contain a list of all SEC filings, including the filer CIK, form type, and form location in a given quarter. These are downloaded in 1_get_master_files.R.
   * From these master files, a file is created for each CIK that files a Form 13F. This file contains the form, report date, and location of the file in the EDGAR database. These are stored in the subdirectories "all_13f," which contains all 13F filings (including notices and amendments), and "only_13f_hr," which contains only original 13F filings. These subdirectories are created in 2_create_13f_universe.
4. raw_filings: This folder contains the plain text 13F filings, stored in subdirectories by filer CIK. These forms are downloaded in 3_process_all_ciks.R or 3_process_one_cik.R (to be used in parallel with SLURM).
5. raw_tables: This folder contains the plain text information tables from the 13F filings, stored in subdirectories by filer CIK. These are extracted from the raw filings in 3_process_all_ciks.R or 3_process_one_cik.R (to be used in parallel with SLURM).
6. processed_tables: This folder contains the information tables that have been cleaned and processed into comma-separated formatted, stored in subdirectories by filer CIK. These are processed from the raw tables in 3_process_all_ciks.R or 3_process_one_cik.R (to be used in parallel with SLURM).
7. biographical: This folder contains a csv for each filer CIK that contains biographical information and a list of 13F filings submitted. These are extracted from the raw filings in 3_process_all_ciks.R or 3_process_one_cik.R (to be used in parallel with SLURM).
8. final_tables: This folder contains the combined biographical data (biographical.csv), the raw holdings data (holdings_raw.csv) before filters are applied, and the final holdings data (holdings.csv) that applies these filters. These are created in 4_combine_tables.R
9. errors.csv: This file contains a list of filings that encountered errors in the parsing process. This is created in 3_process_all_ciks.R or 3_process_one_cik.R (to be used in parallel with SLURM).
