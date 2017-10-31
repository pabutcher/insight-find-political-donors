#!/usr/bin/python
#Load packages
import os
import pandas as pd
import numpy as np

#Setup file paths
scrdir = os.path.dirname(os.path.realpath('file'))
infile = os.path.abspath(os.path.realpath(os.path.join(scrdir, '../input/itcont.txt')))
outzipfile = os.path.abspath(os.path.realpath(os.path.join(scrdir, '../output/medianvals_by_zip.txt')))
outdatefile = os.path.abspath(os.path.realpath(os.path.join(scrdir, '../output/medianvals_by_date.txt')))

#Check if input file present and return error if not
if os.path.isfile(infile)==False:
    warnmsg = "Error: No input file present!!! Expected input file at " + infile + ". Exiting."
    print(warnmsg)
    exit()

 #Check if output files exists and create empty files if not
if os.path.isfile(outzipfile)==False:
    file = open(outzipfile,"w")
    file.close()
if os.path.isfile(outdatefile)==False:
    file = open(outdatefile,"w")
    file.close()

#Read input file into dataframe
inheaders = ["CMTE_ID","AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE","EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"]
indata = pd.read_csv(infile, sep="|", header = None, names = inheaders)
indata=indata[pd.isnull(indata.OTHER_ID)].reset_index(drop=True) #Ignore where OTHER_ID is NOT BLANK. Reset index.

#Check if databank variable exists and create if not
DB_header = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT','TRANSACTION_AMT']
if ('DataBank' in vars()) == False:
    DataBank=pd.DataFrame(columns=DB_header)

#Main loop
#Using for loop to simulate streaming data
for x in range(len(indata)):
    #Get current zip and CMTE. Saves time as will call more than once.
    in_zip = int(str(indata.ZIP_CODE[x])[:5])
    in_CMTE_ID = indata.CMTE_ID[x]
    #Update databank
    DataBank=DataBank.append(pd.DataFrame([[in_CMTE_ID,in_zip,indata.TRANSACTION_DT[x],indata.TRANSACTION_AMT[x]]],columns=DB_header),ignore_index=True)
    #Get values for zipcode file
    ziptrans=DataBank.TRANSACTION_AMT[(DataBank.CMTE_ID==in_CMTE_ID) & (DataBank.ZIP_CODE==in_zip)] # get transactions for zip and CMTE
    #calculate vals
    median_fromzip=int(np.rint(np.median(ziptrans)))
    num_fromzip=len(ziptrans)
    tot_fromzip=np.sum(ziptrans)
    #append zipcode file
    file = open(outzipfile,"a")
    file.write(in_CMTE_ID + '|' + str(in_zip) + '|' + str(median_fromzip) + '|' + str(num_fromzip) + '|' + str(tot_fromzip) + '\n')
    file.close()