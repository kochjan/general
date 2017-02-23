#!/opt/python2.7/bin/python

import requests
import urllib
import BeautifulSoup as bs
import time
import random
from datetime import datetime

import csv
import re
import os

tdnetBase = 'www.release.tdnet.info/inbs/'
downloadBase =  "/home/wzhu/download_daily/"


def getIx(node, coordList):
    '''Get a descendant node from a starting ancestor node and a list of coordinates:
        Inputs:
        node: starting ancestor node
        coordList: e.g. [1,2,3,2,1], the path of child IDs toward the desired descendant
        Output:
        descendantNode
    '''
    while len(coordList)>0:
        idx=coordList.pop(0)
        if (node and node.contents and len(node.contents)>idx):
            node=node.contents[idx]
        else:
            node = None
            break
    return node

def cleanTDnet(ucode):
    '''TDnet landing page contains bad html code that messes up BeautifulSoup parser '''
    u2=ucode.replace('<!-I_TMP_TABLE_FORMAT_01.html start->','').replace('<!-I_TMP_TABLE_FORMAT_01.html end->','')
    u2=u2.replace('<!-I_TMP_TABLE_FORMAT_11.html start->','').replace('<!-I_TMP_TABLE_FORMAT_11.html end->','')
    u2=u2.replace('<!-I_TMP_LIST_FRAME.html end->','').replace('<!-I_TMP_LIST_FRAME.html start->','')
    return u2



def getOutDir(date):
    return downloadBase + tdnetBase + "{:%Y%m%d}/".format(date)

def getDailyUrl(date, page):
    url= 'https://' + tdnetBase + 'I_list_{0:03d}'.format(page) + '_{:%Y%m%d}.html'.format(date)
    return url

def getContentUrl(file):
    return 'https://' + tdnetBase + file

def getFilename(url):
    m = re.search(r'([^\/]+)$', url)
    return m.group()

def getSoup(req):
    u= req.content.decode("utf8")
    u2 = cleanTDnet(u)    
    return bs.BeautifulSoup(u2)

def saveUrl(req, filepath):
    if req.status_code == 200:
        with open(filepath, 'w') as outfile:
            outfile.write(req.content)
    return req

def getLastPage(soup):
    '''This finds the number of index pages for a day'''
    pages=getIx(soup,[2,3,1,1,1,1,1,1,1,5,1])
    if pages:
        last = 1 + len(pages.findAll(name='div', attrs={'class':'pager-M'}))
        return last
    else:
        return -1

def tableToCSV(tableNode, filepath):
    rows = []
    for row in tableNode.findAll('tr'):
        rowlist = [ col.text.encode('utf8') for col in row.findAll(re.compile('t[dh]'))]
        for link in row.findAll('a'):
            rowlist.append(link['href'])
        rows.append(rowlist)
    with open(filepath, 'wb') as f:
        writer = csv.writer(f)
        writer.writerows(row for row in rows if row)

def saveContent(list, outdir):
    for item in list:
        file = item['href']
        url = getContentUrl(file)
        urllib.urlretrieve(url, outdir+file)
        time.sleep(random.normalvariate(4,0.5))

import glob
def run(date, startPage=1):
    # create OutDir
    dir = getOutDir(date)
    if not os.path.exists(dir):
        os.makedirs(dir)

    if glob.glob(dir+"/*csv"):
        print "TDnet download may already completed for {}".format(date)
        print "Empty the output directory to re-download."
        return

    # we start with the landing page, and extend the lastpage range later on
    lastPage = startPage
    currentPage = startPage

    while currentPage <= lastPage:
        # download page and save it
        url = getDailyUrl(date, currentPage)
        req = requests.get(url)
    
        filepath = dir + getFilename(url)
        saveUrl(req, filepath)
    
    	# save content table to CSV
        soup = getSoup(req)
        contentNode = getIx(soup, [2,3,1,1,1,1,3,1])
        if not contentNode:
            print "something is wrong: cannot get contentNode for {}".format(filepath)
            break
        tablepath = filepath+'.csv'
        tableToCSV(contentNode, tablepath)

    	# save content to same dir
        contentList = contentNode.findAll('a')
        saveContent(contentList, dir)

        # fix lastPage from page info
        lastPage = getLastPage(soup)
        if lastPage < 0:
            print "cant find last page"
        currentPage += 1

usage = '''
%cd ~/gitlabs/data-collection/tdnet.jpn/
import tdnet_live
import pandas as pd
from datetime import datetime as dd

tdnet_live.run(dd(2015,3,2))

# to start on the 2nd page:
tdnet_live.run(dd(2015,3,2),2)

for date in pd.DateRange(dd(2015,2,2), dd(2015,2,15),offset=pd.datetools.day):
    tdnet_live.run(date)

'''
