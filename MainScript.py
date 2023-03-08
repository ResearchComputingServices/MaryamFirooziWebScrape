import os
import re
import traceback
import sys

from bs4 import BeautifulSoup
import requests 
from urllib.parse import urlparse
from requests_html import HTMLSession
from urllib.parse import urlparse, urljoin


############################################################################################

BASE_FOLDER_PATH = "/home/jazminromero/zip"
#BASE_FOLDER_PATH = '/home/nickshiell/Documents/ScrappyTestApp/MaryamFirooziWebScrape'
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

def ScrapeLinks(soup, url, currentDomain):
    linkList = []

    # Get all the links on the page
    #links = soup.find_all(['a','link'], href=True)

    # remove all links that go out of the 'currentDomain'
    #for link in links:
    #    linkDomain = urlparse(link['href']).netloc
        
    #    if currentDomain == linkDomain or linkDomain=="":
    #        linkList.append(link['href'])

    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in linkList:
            # already in the set
            continue
        if currentDomain not in href:
            # external link
            continue
        linkList.append(href)

    return linkList
    

############################################################################################
# This function takes in a URL and returns a BeautifulSoup parsed object

def ParseWebpage(webpageURL):

    try:
        #webPage = requests.get(webpageURL)           ======> Original code
        #webPageParsed = BeautifulSoup(webPage.content, 'html.parser')   =======> Original code

        # initialize an HTTP session
        session = HTMLSession()
        # make HTTP request & retrieve response
        webPage = session.get(webpageURL)
        # execute Javascript
        try:
            webPage.html.render()
        except:
            pass
        webPageParsed = BeautifulSoup(webPage.html.html, "html.parser")

    except:
        webPageParsed = ""
        print("Error on parse: " + webpageURL)
        print(sys.exc_info()[0])
        traceback.print_exc()

    return webPageParsed



############################################################################################
# This function removes all text which is contained between <> ie. html tags

def CleanSoup(soup):
 
    for data in soup(['style', 'script']):
        # Remove tags
        data.decompose()
 
    # return data by retrieving the tag content
    return ' '.join(soup.stripped_strings)

############################################################################################

def CrawlUrl(url):
    currentDomain = urlparse(url).netloc
    print('Crawling: ', url)
    print('Domain: ', currentDomain)
    
    crawlledLinks = []
    linksToCrawl = [url]    

    websiteText = ''

    crawlCounter = 0
    for nextLink in linksToCrawl:

        textOnly = ""
        # grab all the html from the supplied url
        soup = ParseWebpage(nextLink) 
  
        # add nextLink to the list of links already crawled
        crawlledLinks.append(nextLink)
        
        # remove nextLink from the list of links TO crawl
        linksToCrawl.remove(nextLink)
                
        # Harvest new links
        # find new links and add them to the list of links to crawl if conditions met
        if len(soup)>0:
            newLinksList = ScrapeLinks(soup = soup, url = url, currentDomain=currentDomain)
            for newLink in newLinksList:
                # only add the new link if it hasn't already been crawled and it is not already schedule to be crawled
                if newLink not in crawlledLinks and newLink not in linksToCrawl:
                    linksToCrawl.append(newLink)

            # Get only the text from the html (ie remove all html tags)
            textOnly = CleanSoup(soup)

        # add the new text to the 
        websiteText += textOnly
        
        # Update the depth of the crawl and end the search if MAX_CRAWL_DEPTH has been reached
        crawlCounter += 1

        print('Current Link:', nextLink)
        print('Depth: ', crawlCounter)
        print('To Crawl: ', len(linksToCrawl))
        print('# chars: ', len(websiteText))
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

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
        print (counter)
        print ("============================================================================")
        websiteText = CrawlUrl(companyDict[company])
        listOfFoundKeyWords = SearchForKeyWords(websiteText,keywordList)
        WriteResults(company,listOfFoundKeyWords)
        counter = counter + 1
    except:
        print("Error on parse: " + company)
        print(sys.exc_info()[0])
        traceback.print_exc()

