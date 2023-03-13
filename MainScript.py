import os
import io
import traceback
import sys
import time

from urllib.parse import urlparse
from requests_html import HTMLSession
from urllib.parse import urlparse
from PyPDF2 import PdfReader

############################################################################################

#BASE_FOLDER_PATH = "/home/jazminromero/zip"
BASE_FOLDER_PATH = '/home/nickshiell/Documents/Work/MARYAM_FIROOZI_PROJECT/MaryamFirooziWebScrape'
INPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'InputData')
COMPANY_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'companyWebsites.csv')
KEYWORDS_DATA_FILE_PATH = os.path.join(INPUT_DATA_FOLDER_PATH,'keywords.dat')
OUTPUT_DATA_FOLDER_PATH = os.path.join(BASE_FOLDER_PATH,'OutputData')

COMPANY_NAME_COL_NUM = 1
COMPANY_URL_COL_NUM = 6

MAX_CRAWL_DEPTH = 10000

############################################################################################
def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


############################################################################################

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

def ScrapeLocalLinks(webPageResponse):
    
    allLinkList = webPageResponse.html.absolute_links
    baseURLDomain = urlparse(webPageResponse.html.base_url).netloc
    
    localLinkList = []

    for link in allLinkList:
        if  urlparse(link).netloc == baseURLDomain:
            if is_valid(link):
                localLinkList.append(link)

    return localLinkList
    
############################################################################################
# This function takes in a URL and returns a requests-html 'Response' object
def RequestWebPage(webpageURL):

    webPageRequest = ''

    try:
        # initialize an HTTP session
        session = HTMLSession()
       
        # make HTTP request & retrieve response
        webPageRequest = session.get(webpageURL)
                
        # execute Javascript        
        try:
            webPageRequest.html.render()
        except:
            pass
        
    except:
        print("Error on parse: " + webpageURL)
        print(sys.exc_info()[0])
        traceback.print_exc()

    return webPageRequest

############################################################################################
# This function removes all text which is contained between <> ie. html tags

def ScrapeText(webPageResponse, url):

    webPageText = ''

    if '.pdf' in url[-4:]:
        print(url)
        f = io.BytesIO(webPageResponse.content)     
        reader = PdfReader(f)
        
        for page in reader.pages:       
            listOfText = page.extractText().split('\n')
            
            for text in listOfText:
                webPageText += text + "\n"
        input('Found PDF...')
    else:
        paragraphs = webPageResponse.html.find('p')   
        for para in paragraphs:
            webPageText += para.text + '\n'
        
    print(webPageText)

    return webPageText

############################################################################################

def CrawlUrl(url, VERBOSE = True):
    currentDomain = urlparse(url).netloc
    print('Crawling: ', url)
    print('Domain: ', currentDomain)
    
    crawlledLinks = []
    linksToCrawl = [url]    

    websiteText = ''

    crawlCounter = 0
    while len(linksToCrawl) > 0:
        s = time.time()
         
        # remove nextLink from the list of links TO crawl
        nextLink = linksToCrawl.pop()
        
        # grab all the html from the supplied url
        webPageRequest = RequestWebPage(nextLink) 
  
        # add nextLink to the list of links already crawled
        crawlledLinks.append(nextLink)
                        
        # Harvest new links
        # find new links and add them to the list of links to crawl if conditions met
        if len(webPageRequest.text)>0:
            newLinksList = ScrapeLocalLinks(webPageRequest)
            
            for newLink in newLinksList:
                # only add the new link if it hasn't already been crawled and it is not already schedule to be crawled
                if newLink not in crawlledLinks and newLink not in linksToCrawl:
                    linksToCrawl.append(newLink)
                    
        # add the new text to the 
        websiteText += ScrapeText(webPageRequest, nextLink)
        
        # Update the depth of the crawl and end the search if MAX_CRAWL_DEPTH has been reached
        crawlCounter += 1
        e = time.time()
        
        if VERBOSE: 
            print('Current Link:', nextLink)
            print('Depth: ', crawlCounter)
            print('To Crawl: ', len(linksToCrawl))
            print('# chars: ', len(websiteText))
            print('time: ', e-s)
            print('=====================================================================')        
        else:
            print(crawlCounter, len(linksToCrawl), e - s)

        if crawlCounter > MAX_CRAWL_DEPTH:
            print('[WARNING]: CrawlUrl: MAX_CRAWL_DEPTH reacehd:', MAX_CRAWL_DEPTH)
            break
            
    return websiteText
    
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

companyDict = GetCompanyDict()
keywordList = GetKeyWordList()

print('# of Companies:', len(companyDict))

counter = 1

for company in companyDict.keys():
    try:
        print('Company Counter: ', counter)
        websiteText = CrawlUrl(companyDict[company])
        listOfFoundKeyWords = SearchForKeyWords(websiteText,keywordList)
        WriteResults(company,listOfFoundKeyWords)
    except:
        print("Error on parse: " + company)
        print(sys.exc_info()[0])
        traceback.print_exc()
    
    counter = counter + 1
    
    break

