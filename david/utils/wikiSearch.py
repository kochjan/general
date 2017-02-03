import sys
import csv
import string
import urllib
import urllib2
from bs4 import BeautifulSoup
import re

###
# Usage: python jpnreader.py [inputfile] [outputfile] 
# Reads csv inputfile and searches for the companies listed on Japanese-language
# Wikipedia.  Writes Wikipedia urls to outputfile.
###

def readFile(fname, nameCol, locIDCol, barrIDCol, dropLabel = True):
    """ 
    Reads in source csv file fname.  nameCol, locIDCol, and barrIDCol specify
    columns containing the company name, LOCID, and BARRID.  Default is to drop
    the first row, assuming it to contain column labels.  Returns a zipped list
    of tuples (company name, BARRID, LOCID).
    """

    compName = []
    locID = []
    barrID = []

    with open(fname, 'rU') as file:
        reader = csv.reader(file, delimiter=',')
        for row in reader:
            compName.append(string.rstrip(row[nameCol]))
            locID.append(string.rstrip(row[locIDCol]))
            barrID.append(string.rstrip(row[barrIDCol]))

    if dropLabel:
        companies = zip(compName, barrID, locID)[1:] 
    return companies

def checkPage(url, locID):
    """
    Scrapes a page at specified url searching for instances of locID inside <a>,
    <b>, or <td> tags.
    """
    strpLocID = re.sub("[^0-9]", "", locID)
    resource = opener.open(url)
    data = resource.read()
    resource.close()
    soup = BeautifulSoup(data)

    tags = soup.findAll(['a', 'b', 'td'], recursive=True)
    for t in tags:
        if t.contents and str(strpLocID) in t.contents[0]:
            return True, url
    return False, ''

def checkLinks(results, locID):
    """
    Follows the hyperlink tags in results and checks them for instances of
    locID.
    """
    for result in results:
        url = 'http://ja.wikipedia.org' + result.find('a').get('href')
        check = checkPage(url, locID)
        if check:
            return check
    return False, ''

def searchCompanies(companies, prefix, suffix, verbose=True):
    """
    Searches for each company in companies by inserting the company name between
    prefix and suffix to create a wikipedia search url.  Default verbosity
    indicates success or failure after each search.
    """

    tagged = []
    matches = 0
    for comp in companies:
        try:
            searchCenter = string.replace(comp[0], ' ', '+')
            urlString = prefix + searchCenter + suffix

            resource = opener.open(urlString)
            data = resource.read()
            resource.close()
            soup = BeautifulSoup(data)

            valid = checkPage(urlString, comp[2])
            if valid[0] is not True:
                # Can raise limit if necessary.
                results = soup.findAll('div', {'class':'mw-search-result-heading'}, limit=5)
                valid = checkLinks(results, comp[2])

            tagged.append(comp + valid) 

            if verbose:
                if valid[0]:
                    matches += 1
                    print "Found " + comp[0]
                else:
                    print "Failed to find " + comp[0]
        except: 
            print 'Error: ' + comp[0]
            continue

    print matches, "companies tagged out of", len(companies)
    return tagged

opener = urllib2.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')] 

# Currently set up to search japanese wikipedia.
searchPrefix = 'http://ja.wikipedia.org/w/index.php?search='
searchSuffix = '&title=%E7%89%B9%E5%88%A5%3A%E6%A4%9C%E7%B4%A2'

# Arguments to readFile can be changed for different input file formats. 
companies = readFile(sys.argv[1], 1, 3, 0)
companies = companies[0:10] # Remove this to run full list.

tagged = searchCompanies(companies, searchPrefix, searchSuffix, verbose=True)

with open(sys.argv[2], 'wb') as result:
    writer = csv.writer(result, dialect='excel')
    writer.writerows([('Company', 'BARRID', 'LOCID', 'Tagged', 'url')])
    writer.writerows(tagged)
