# Owner information
Created by Peter Butcher for the Insight Data Engineering fellowship

# Dependencies
Requires the following packages: pandas, numpy, datetime, os and sys

# Run Command
python ./src/find_political_donors.py [INPUTFILES] [OUTPUTZIPFILE] [OUTPUTDATEFILE]

For example:
'python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt'

Or by using run.sh in main folder
'bash run.sh'

# Input file
Input file is expected to be pipe '|' separated with each line a new donation. Columns are in order:  "CMTE_ID","AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE","EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"

## The following values are ignored
Only lines with "OTHER_ID" empty are used. All other donations are ignored and not used in output files. Lines with either "CMTE_ID" or "TRANSACTION_AMT" equal to zero are ignored and not used in output. If "ZIP_CODE" is invalid the value is not used for zipcode calculations, but IS used for date calculations. If "TRANSACTION_DT" is invalid the value is not used for date calculations, but IS used for zipcode calculations.

# Output files
1) medianvals_by_zip: Each line contains a new donation with a)recipient(CMTE_ID), b)zipcode, c)median from zipcode  for recipient, d)number of donations from zipcode for recipient, and e)total amount donated from zipcode to recipient. In that order.
2) medianvals_by_date: Each line contains a unique a)recipient (CMTE_ID) and b) Date combination, with c)median for date and recipient, d)number of donations for date and recipient and e) total donations for date and recipient.

# Approach
As challenge was to create streaming data file function uses a for loop to go through input file and rewrites after each line, as though that line was a new input. medianvals_by_zip is appended on each loop as is running median, while medianvals_by_date is entirely rewritten as is summary of all data. Data is stored in variable 'DataBank' which if brought outside the main loop could be stored for future iterations, currently treated as only one execution of script

