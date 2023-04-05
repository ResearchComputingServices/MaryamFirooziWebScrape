import os

OUTPUT_DATA_DIR = '/home/nickshiell/MaryamFirooziWebScrape/OutputData'
SEND_DATA_DIR = '/home/nickshiell/MaryamFirooziWebScrape/FilesToSend/'

def GetFileSize(filePath):
    fileStats = os.stat(filePath)
    return fileStats.st_size

for f in os.listdir(OUTPUT_DATA_DIR):
    f = os.path.join(OUTPUT_DATA_DIR, f)
    if os.path.isfile(f):
            size = GetFileSize(f)
            
            if size > 10000:
                cmdString = 'mv \''+f+'\' '+SEND_DATA_DIR
                os.system(cmdString)
    
