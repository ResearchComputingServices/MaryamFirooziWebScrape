import os
import io
import traceback
import sys
import time
import asyncio
import subprocess

import pandas as pd
from urllib.parse import urlparse
from PyPDF2 import PdfReader

from requests_html import AsyncHTMLSession

############################################################################################

BASE_FOLDER_PATH = './'
INPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'InputData')
COMPANY_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'companyWebsites.csv')
KEYWORDS_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'keywords.dat')
OUTPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'OutputData')

COMPANY_NAME_COL_NUM = 1
COMPANY_URL_COL_NUM = 6

MAX_CRAWL_DEPTH = 10000
MAX_CRAWL_LIST = 100
MAX_RENDER_TIME_OUT = 60
WAIT_TIME = 3
GET_TIME_OUT = 30

############################################################################################
def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

############################################################################################
def IsPDF(url):
    isPDF = False
    
    if '.pdf' in url: 
        isPDF = True
    
    return isPDF

############################################################################################
# This function reads all the companies and their URLs into a dict (name:url)
def GetCompanyDict(dataFileName = COMPANY_DATA_FILE_PATH, verboseFlag = False):
 
    # Store the company names (key) and urls (values) in a dictionary
    companyDict = {}

    companyDataFile = open(dataFileName, 'r')
    lines = companyDataFile.readlines()

    for line in lines:
        lineSplit = line.split('^')
                
        if len(lineSplit) > COMPANY_URL_COL_NUM:
            companyName = lineSplit[COMPANY_NAME_COL_NUM]
            companyURL = lineSplit[COMPANY_URL_COL_NUM]
        
            companyDict[companyName] = companyURL
        else:
            if verboseFlag:
                print('[WARNING]: GetCompanyDict(): data missing on line',counter)
                print('line data: ', line)
    
    return companyDict

############################################################################################
# This function reads the keywords into a list
def GetKeyWordList():
    keywordList = []
    
    keywordDataFile = open(KEYWORDS_DATA_FILE_PATH, 'r')
    lines = keywordDataFile.readlines()

    for keyword in lines:
        if "\n" in keyword:
            keyword_without_new_line = keyword.strip("\n")
            keyword = keyword_without_new_line
        keywordList.append(keyword.strip())
    
    return keywordList

############################################################################################
# This function scrapes all the links on the page which are in the same domain has the url
def ScrapeLocalLinks(webPageResponse, url):
    
    localLinkList = []
    
    if IsPDF(url):
            return localLinkList
    
    try:
        allLinkList = webPageResponse.html.absolute_links
        baseURLDomain = urlparse(webPageResponse.html.base_url).netloc
        
        localLinkList = []

        for link in allLinkList:
            if  urlparse(link).netloc == baseURLDomain:
                if is_valid(link):
                    localLinkList.append(link)
    except:
        # print(webPageResponse.content)
        return localLinkList
        
    return localLinkList
    
############################################################################################
# This function collects all the text in <p> tags or in a pdf document
def ScrapeText(webPageResponse, url):

    webPageText = ''

    if IsPDF(url):
        try:
            f = io.BytesIO(webPageResponse.content)     
            reader = PdfReader(f)
        
            for page in reader.pages:       
                listOfText = page.extractText().split('\n')
            
                for text in listOfText:
                    webPageText += text + "\n"
        except:
            return webPageText
    else:       
        if webPageResponse.html != None:
            try:
                paragraphs = webPageResponse.html.find('p')   
                for para in paragraphs:
                    webPageText += para.text + '\n'
            except:
                print('Empty Response...')
                
    return webPageText

############################################################################################

async def Get(asyncSession, url, VERVOSE = False):
    if(VERVOSE):
        print('Start Get: ', url, flush=True)
           
    try:
        response  = await asyncSession.get(url, timeout = GET_TIME_OUT)
        # print(f"Status Code: {response.status_code}")

    except:
        response = None
    
    if(VERVOSE):
        print('End Get: ', url, flush=True)
    
    return response
    
def TaskFunctionGetResponses(asyncSession, urlsToGet):
    tasks = []
    
    for url in urlsToGet:
        tasks.append(Get(asyncSession, url))
        
    return tasks

############################################################################################

async def Render(response, VERVOSE = False):
    
    if(VERVOSE):
        print('Start Rendering: ', response.url, flush=True)
    
    if not IsPDF(response.url):  
        try:      
            await response.html.arender(timeout=MAX_RENDER_TIME_OUT, wait=WAIT_TIME)
        except:
            print('Failed to render:',response.url) 
            # print(sys.exc_info()[0])
            # traceback.print_exc()   
    
    if(VERVOSE):
        print('Done Rendering: ', response.url, flush=True)
    
    return response

def TaskFunctionRender(listOfResponses): 
    
    tasks = []
    
    for response in listOfResponses:
        if not IsPDF(response.url):
            tasks.append(Render(response))

    return tasks

############################################################################################

async def TaskManager(linksToCrawl):
    
    # # initialize an HTTP session
    session = AsyncHTMLSession()
    
    getTasks = TaskFunctionGetResponses(session, linksToCrawl)   
    responses = await asyncio.gather(*getTasks)

    # renderTasks = TaskFunctionRender(responses)
    # responses = await asyncio.gather(*renderTasks)

    await session.close()
    
    return responses

############################################################################################

def KillChromePocesses(VERBOSE = False):
     
    if VERBOSE:
        print('Killing chrome process...')
    
    cmd = "ps -u nickshiell | grep chrome | wc -l"
    
    nProcess = int(subprocess.check_output(cmd, shell=True,text=True))
    
    attempts = 0
    
    while nProcess > 0 and attempts <= 10:
        if VERBOSE:
            print('# of Process: ', nProcess,'\t# of Attempts:',attempts)
        
        os.system("pkill -9 chrome")
       
        nProcess = int(subprocess.check_output(cmd, shell=True,text=True))
        attempts += 1
        time.sleep(1)

############################################################################################
   
def CrawlUrl(url, logFile):
    currentDomain = urlparse(url).netloc
    logFile.write('Crawling: '+ url +'\n')
    print('Crawling: ', url)
    logFile.write('Domain: '+ currentDomain+'\n')
    print('Domain: ', currentDomain)
    
    crawledLinks = []
    linksToCrawl = [url]    

    crawlCounter = 0
    failedURL = 0

    websiteTextDF = pd.DataFrame(columns=['URL','Text','Keywords'])
        
    while len(linksToCrawl) > 0:
        s = time.perf_counter()
       
        # Create a list of up 100 links to crawl
        nLinksCrawled = 0
        crawlList = []
        while len(crawlList) <= MAX_CRAWL_LIST and len(linksToCrawl) > 0:
            crawlList.append(linksToCrawl.pop())
            nLinksCrawled += 1
        
        # send the list of links to crawl to the TaskManager function
        results = asyncio.run(TaskManager(crawlList))

        # KillChromePocesses()

        # create a list of links and text found by the Tasks
        listOfFoundLinks = []
        for r in results:
            if r != None:
                listOfFoundLinks = listOfFoundLinks + ScrapeLocalLinks(r, r.url)
                scrapedText = ScrapeText(r, r.url)
                websiteTextDF.loc[len(websiteTextDF)] = [r.url,scrapedText,'']
            else:
                failedURL += 1
        
        # update the list of links that have been crawled
        for link in crawlList:
            crawledLinks.append(link) 
        
        # add the new found links only if they are not in the list AND not already visited
        for newLink in listOfFoundLinks:
            if newLink not in crawledLinks and newLink not in linksToCrawl:
                linksToCrawl.append(newLink)
                                             
        e = time.perf_counter()
        
        # Update the depth of the crawl and end the search if MAX_CRAWL_DEPTH has been reached
        crawlCounter += nLinksCrawled

        logFile.write(str(crawlCounter)+','+str(nLinksCrawled)+','+str(len(linksToCrawl))+','+str(failedURL)+','+str(e-s)+'\n')
        print(crawlCounter,',',nLinksCrawled,',',len(linksToCrawl),',',failedURL,',',(e-s))

        if crawlCounter >= MAX_CRAWL_DEPTH:
            logFile.write('[WARNING]: CrawlUrl: MAX_CRAWL_DEPTH reacehd: '+ str(MAX_CRAWL_DEPTH)+'\n')
            print('[WARNING]: CrawlUrl: MAX_CRAWL_DEPTH reacehd: ', MAX_CRAWL_DEPTH)
            break
            
    return websiteTextDF, crawledLinks
    
############################################################################################

def SearchForKeyWords(websiteTextDF, keywordList):   
    
    for index, row in websiteTextDF.iterrows():
        websiteText = row['Text']        
        websiteText = websiteText.lower()
        
        foundKeywordsList = []        
        for keyword in keywordList:
            searchTerm = ' ' + keyword.lower() + ' '
            if searchTerm in websiteText:
                foundKeywordsList.append(keyword.lower())        
        
        row['Keywords'] = foundKeywordsList
        
    return websiteTextDF
   
############################################################################################

def WriteResults(company,listOfFoundKeyWords):
    outputFilePath = os.path.join(OUTPUT_DATA_FOLDER_PATH,company+'.txt')
    outputDataFile = open(outputFilePath, 'w')
    
    for keyword in listOfFoundKeyWords:
        outputDataFile.write(keyword+'\n')
        
    outputDataFile.close()
    return

############################################################################################

def WriteCrawledLinks(crawledLinks):
    outputFilePath = os.path.join(OUTPUT_DATA_FOLDER_PATH,company+'_crawled_links.txt')
    outputDataFile = open(outputFilePath, 'w')
    
    for link in crawledLinks:
        outputDataFile.write(link+'\n')
        
    outputDataFile.close()
    return

############################################################################################

def WriteScrapedText(webpageText):
    outputFilePath = os.path.join(OUTPUT_DATA_FOLDER_PATH,company+'_scraped_text.txt')
    outputDataFile = open(outputFilePath, 'w')
    
    outputDataFile.write(webpageText)
        
    outputDataFile.close()
    return

############################################################################################

if len(sys.argv) == 3:

    dataFileName = sys.argv[1]
    logFileName = sys.argv[2]

    dataFilePath = os.path.join(INPUT_DATA_FOLDER_PATH, dataFileName) 
    logFilePath = os.path.join(OUTPUT_DATA_FOLDER_PATH, logFileName)

    logFile = open(logFilePath,'a+')

    companyDict = GetCompanyDict(dataFilePath)
    keywordList = GetKeyWordList()


    logFile.write('# of Companies: '+ str(len(companyDict))+ '\n')
    print('# of Companies: ',len(companyDict))
    counter = 1

    for company in companyDict.keys():
        
        try:
            logFile.write('Company Counter: '+ str(counter) + '\n')
            print('Company Counter: ', str(counter))
         
            websiteTextDF, crawledLinks = CrawlUrl(companyDict[company], logFile)        
            websiteTextDF = SearchForKeyWords(websiteTextDF,keywordList)
            
            filename = company.replace(' ', '_')+'.pkl'
            outputFilePath = os.path.join(OUTPUT_DATA_FOLDER_PATH, filename)
            print(outputFilePath)
            websiteTextDF.to_pickle(outputFilePath)
            
        except:
            logFile.write('0 ,0 , -1 , 0 , 0 ,'+company)
            print('0 ,0 , -1 , 0 , 0 ,',company)
            print(sys.exc_info()[0])
            traceback.print_exc()
        
        # input('Press ENTER to continue...')  
        counter = counter + 1
        logFile.flush()
        
    logFile.close()
else:
    print('[ERROR]: No company list, or log file ename given.')

