#!/usr/bin/python
#Load packages
import pandas as pd
import numpy as np
import os, sys, datetime

#Only needs to be run once at beginning
def setup_filestructure(infile,outzipfile,outdatefile):
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

def check_date(indate):
	indate_str=str(indate)
	try:
		d=datetime.datetime.strptime(indate,  '%m%d%Y')
		return(indate_str)
	except ValueError:
		return(-1)

def check_zip(inzip):
	inzip_str=str(inzip)
	if (len(inzip_str) == 9): #long date format
		return(inzip_str[:5])
	elif (len(inzip_str) == 5):
		return(inzip_str)
	else:
		return(-1) #Return -1 if invalid zip

def find_political_donors(inputf,outputz,outputd):
    #Setup file paths
	scrdir = os.path.dirname(os.path.realpath('file'))
	infile = os.path.abspath(os.path.realpath(os.path.join(scrdir,inputf)))
	outzipfile = os.path.abspath(os.path.realpath(os.path.join(scrdir, outputz)))
	outdatefile = os.path.abspath(os.path.realpath(os.path.join(scrdir, outputd)))
	#Setup file structure
	setup_filestructure(infile,outzipfile,outdatefile)
    #Check if databank variable exists and create if not
	DB_header = ['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT','TRANSACTION_AMT']
	if ('DataBank' in vars()) == False:
	    DataBank=pd.DataFrame(columns=DB_header)
	    DataBank.TRANSACTION_AMT=DataBank.TRANSACTION_AMT.astype(float) #set to float
	#Read input file into dataframe
	inheaders = ["CMTE_ID","AMNDT_IND","RPT_TP","TRANSACTION_PGI","IMAGE_NUM","TRANSACTION_TP","ENTITY_TP","NAME","CITY","STATE","ZIP_CODE","EMPLOYER","OCCUPATION","TRANSACTION_DT","TRANSACTION_AMT","OTHER_ID","TRAN_ID","FILE_NUM","MEMO_CD","MEMO_TEXT","SUB_ID"]
	indata = pd.read_csv(infile, sep="|", header = None, names = inheaders,converters={'TRANSACTION_DT': lambda x: str(x),'ZIP_CODE': lambda x: str(x)})
	#Remove bad values
	if (any(pd.isnull(indata.OTHER_ID)==False)):
		indata=indata[pd.isnull(indata.OTHER_ID)].reset_index(drop=True) #Ignore where OTHER_ID is NOT BLANK. Reset index.
	if (any(pd.isnull(indata.CMTE_ID))):
		indata=indata[pd.isnull(indata.CMTE_ID)==False].reset_index(drop=True) #Check for missing CMTE 
	if (any(pd.isnull(indata.TRANSACTION_AMT))):
		indata=indata[pd.isnull(indata.TRANSACTION_AMT)==False].reset_index(drop=True) #Check for missing TRANSACTION_AMT 
	
    #Main loop
	#Using for loop to simulate streaming data
	for x in range(len(indata)):
	    #Get current zip and CMTE. Saves time as will call more than once.
	    in_zip = check_zip(indata.ZIP_CODE[x])
	    in_CMTE_ID = indata.CMTE_ID[x]
	    in_DT = check_date(indata.TRANSACTION_DT[x])
	    #Update databank
	    DataBank=DataBank.append(pd.DataFrame([[in_CMTE_ID,in_zip,in_DT,indata.TRANSACTION_AMT[x]]],columns=DB_header),ignore_index=True)
	    if (int(in_zip)>0): #If ZIPCODE is invalid ignore for zip file
	    	#Get values for zipcode file
	    	ziptrans=DataBank.TRANSACTION_AMT[(DataBank.CMTE_ID==in_CMTE_ID) & (DataBank.ZIP_CODE==in_zip)] # get transactions for zip and CMTE
	    	#calculate vals
	    	median_fromzip=int(np.rint(np.median(ziptrans)))
	    	num_fromzip=len(ziptrans)
	    	tot_fromzip=np.sum(ziptrans)
	    	#append zipcode file
	    	file = open(outzipfile,"a")
	    	file.write(in_CMTE_ID + '|' + str(in_zip) + '|' + str(median_fromzip) + '|' + str(num_fromzip) + '|' + str(int(tot_fromzip)) + '\n')
	    	file.close()
	    #Get vals for date file
	    date_df=DataBank[pd.to_numeric(DataBank.TRANSACTION_DT)>0].groupby(['CMTE_ID', 'TRANSACTION_DT']).agg({'TRANSACTION_AMT' :['median','count','sum']})
	    date_df.columns = ["_".join(x) for x in date_df.columns.ravel()]
	    #update types
	    date_df.TRANSACTION_AMT_median= np.rint(date_df.TRANSACTION_AMT_median).astype(int) #round to nearest int and make int
	    date_df.TRANSACTION_AMT_sum= date_df.TRANSACTION_AMT_sum.astype(int) #make int
	    #rewriting csv each loop to simulate streaming data
	    date_df.to_csv(outdatefile, sep='|', header=False, index=True, index_label=None, mode='w', encoding=None, compression=None, quoting=None, quotechar='"', line_terminator='\n', chunksize=None, tupleize_cols=False, date_format=None, doublequote=True, escapechar=None, decimal='.')

if __name__== "__main__":
	find_political_donors(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]))