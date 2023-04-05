import os
import pandas as pd
import pickle

OUTPUT_DATA_DIR = '/home/nickshiell/MaryamFirooziWebScrape/OutputData'
INPUT_DATA_DIR = '/home/nickshiell/MaryamFirooziWebScrape/InputData'

COMPANY_NAME_COL_NUM = 1
COMPANY_URL_COL_NUM = 6

companyDict = {}
pickleList = []

outputFile = open('output.csv','w+')

companyDataFile = open(INPUT_DATA_DIR+'/companyWebsites_FULL_LIST.csv', 'r')
lines = companyDataFile.readlines()

###############################################################################

def GetFileSize(filePath):
    fileStats = os.stat(filePath)
    return fileStats.st_size

###############################################################################

for line in lines:
    lineSplit = line.split('^')
            
    if len(lineSplit) > COMPANY_URL_COL_NUM:
        companyName = lineSplit[COMPANY_NAME_COL_NUM].strip()
        companyURL = lineSplit[COMPANY_URL_COL_NUM].strip()
    
        companyName = companyName.replace('"', '')
        companyName = companyName.replace(' ', '_')
        companyName = companyName.replace('/', '~')

        companyDict[companyName] = companyURL

###############################################################################

for f in os.listdir(OUTPUT_DATA_DIR):
    if os.path.isfile(os.path.join(OUTPUT_DATA_DIR, f)):
        pickleList.append(f)

###############################################################################

for pkl in pickleList:
    
    pklFilePath = os.path.join(OUTPUT_DATA_DIR, pkl)
    try:        
        if GetFileSize(pklFilePath) > 100000:
            continue
        else:
            df = pd.read_pickle(pklFilePath)   
            if len(df.index) < 10:
                key = pkl[:-4]
                if key in companyDict.keys(): 
                    outputFile.write('PH0^'+key+'^PH1^PH2^PH3^PH4'+companyDict[key]+'\n')
                else:
                    print('Company not found:', key)
                    input('Press ENTER to continue...')

    except:
        print('[WARNING]: Could not open pkl file:',pklFilePath)
        input()

outputFile.close()