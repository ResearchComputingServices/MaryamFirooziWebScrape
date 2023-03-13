import os
import io
import traceback
import sys
import time
import asyncio

from urllib.parse import urlparse
from urllib.parse import urlparse
from PyPDF2 import PdfReader

from requests_html import AsyncHTMLSession

############################################################################################

#BASE_FOLDER_PATH = "/home/jazminromero/zip"
BASE_FOLDER_PATH = '/home/nickshiell/Documents/Work/MARYAM_FIROOZI_PROJECT/MaryamFirooziWebScrape'
INPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'InputData')
COMPANY_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'companyWebsites_test.csv')
KEYWORDS_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'keywords.dat')
OUTPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'OutputData')

COMPANY_NAME_COL_NUM = 1
COMPANY_URL_COL_NUM = 6

MAX_CRAWL_DEPTH = 25

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
    
    if '.pdf' in url[-4:]: 
        isPDF = True
    
    return isPDF

############################################################################################
# This function reads all the companies and their URLs into a dict (name:url)
def GetCompanyDict(verboseFlag = False):
 
    # Store the company names (key) and urls (values) in a dictionary
    companyDict = {}

    companyDataFile = open(COMPANY_DATA_FILE_PATH, 'r')
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
        paragraphs = webPageResponse.html.find('p')   
        for para in paragraphs:
            webPageText += para.text + '\n'

    return webPageText

############################################################################################
# This function respresents all the work that will be done by an async Task when processing
# a url

async def TaskFunction(asyncSession, url):
   
    webPageReponse = None

    try:
        # make HTTP request & retrieve response
        webPageReponse = await asyncSession.get(url)
                
        # execute Javascript
        if not IsPDF(url):  
            try:      
                await webPageReponse.html.arender(timeout=60)
            except:
                return webPageReponse, url
                # print('Failed to render: ',url)   
                # print(sys.exc_info()[0])
                # traceback.print_exc()
                
    except:
        return webPageReponse, url
        # print("Error on parse: " + url)
        # print(sys.exc_info()[0])
        # traceback.print_exc()
    
    return webPageReponse, url

############################################################################################

async def TaskManager(linksToCrawl):
    
    # initialize an HTTP session
    session = AsyncHTMLSession()
    
    listOfTasks = ( TaskFunction(session, url) for url in linksToCrawl)  

    return await asyncio.gather(*listOfTasks)

############################################################################################

def CrawlUrl(url):
    currentDomain = urlparse(url).netloc
    print('Crawling: ', url)
    print('Domain: ', currentDomain)
    
    crawledLinks = []
    linksToCrawl = [url]    

    websiteText = ''
    crawlCounter = 0
    failedURL = 0
    
    while len(linksToCrawl) > 0:
        s = time.perf_counter()
       
        # Create a list of up 100 links to crawl
        nLinksCrawled = 0
        crawlList = []
        while len(crawlList) <= 50 and len(linksToCrawl) > 0:
            crawlList.append(linksToCrawl.pop())
            nLinksCrawled += 1
        
        # send the list of links to crawl to the TaskManager function
        results = asyncio.run(TaskManager(crawlList))
        
        # create a list of links and text found by the Tasks
        listOfFoundLinks = []
        listOfText = []
        for response, currentURL in results:
            if response != None:
                listOfFoundLinks = listOfFoundLinks + ScrapeLocalLinks(response, currentURL)
                listOfText.append(ScrapeText(response,currentURL))
            else:
                failedURL += 1
        
        # update the list of links that have been crawled
        for link in crawlList:
            crawledLinks.append(link) 
        
        # add the new found links only if they are not in the list AND not already visited
        for newLink in listOfFoundLinks:
            if newLink not in crawledLinks and newLink not in linksToCrawl:
                linksToCrawl.append(newLink)
              
        # add the found text to the string which will be searched for key words
        for text in listOfText:
            websiteText += text + '\n'
                                
        e = time.perf_counter()
        
        # Update the depth of the crawl and end the search if MAX_CRAWL_DEPTH has been reached
        crawlCounter += nLinksCrawled

        print(crawlCounter,',',nLinksCrawled,',',len(linksToCrawl),',',failedURL,',',len(websiteText),',',e-s)

        if crawlCounter > MAX_CRAWL_DEPTH:
            print('[WARNING]: CrawlUrl: MAX_CRAWL_DEPTH reacehd:', MAX_CRAWL_DEPTH)
            break
            
    return websiteText, crawledLinks
    
############################################################################################
   
def SearchForKeyWords(websiteText, keywordList):
    foundKeywordsList = []
    
    websiteText = websiteText.lower()
    
    for keyword in keywordList:
        if keyword.lower() in websiteText:
            foundKeywordsList.append(keyword.lower())
            
    return foundKeywordsList
    
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
companyDict = GetCompanyDict()
keywordList = GetKeyWordList()

print('# of Companies:', len(companyDict))

counter = 1

for company in companyDict.keys():
    try:
        print('Company Counter: ', counter)
        
        websiteText, crawledLinks = CrawlUrl(companyDict[company])        
        listOfFoundKeyWords = SearchForKeyWords(websiteText,keywordList)
        
        WriteResults(company,listOfFoundKeyWords)
        WriteCrawledLinks(crawledLinks)
        WriteScrapedText(websiteText)
    except:
        print("Error on parse: " + company)
        print(sys.exc_info()[0])
        traceback.print_exc()
    
    counter = counter + 1


