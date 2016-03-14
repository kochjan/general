#!/opt/python2.7/bin/python
import pandas as pd
from datetime import datetime as dd
import pickle
import requests
import BeautifulSoup as bs
import re
import os
import sys
sys.path.insert(0,'/home/wzhu/gitlabs/data-collection/utils')
import treeViewer
import common
import time

ETNET_TYPES=['hot', 'hotquote', 'business']
DEFAULT_CAT_NUM = 53

def getSoupFromFile(date,categ, altFlag=False):
    dateStr = "{:%Y%m%d}".format(date)
    if altFlag: #test date.0 at other times
        dateStr += ".0"
    if categ in ['hot', 'hotquote', 'business']:
        categStr = categ + '.php'
    else:
        categStr = 'business.php?business={}'.format(categ)
    file = "/home/wzhu/download_daily/www.etnet.com.hk/{}/www%tc%stocks/sector_{}".format(dateStr, categStr)
    if not os.path.exists(file):
        return None
    with open(file, 'r') as fh: 
        soup = pickle.load(fh)
    return soup

def getCategoryNum(soup):
    try:
        t1 = treeViewer.getIx(soup, [2,3,1,15,3]).findAll('table')[2]
        t2 = t1.findAll('a').pop()['href']
        t3 = t2.split('?').pop()
        (t4,t5) = t3.split('=')
        if t4=='business':
            return int(t5)
        else:
            print "cat num format wrong"
            return DEFAULT_CAT_NUM
    except:
        return DEFAULT_CAT_NUM

def getLastUpdate(soup):
    lastUpdates= soup.findAll('div',attrs={'class':"DivBoxStyleD"})
    # treeViewer.getIx(soup, [2,3,1,15,3,11])
    if len(lastUpdates)!=1:
        print "bad format for lastupdate"
        return None
    lastUpdate = lastUpdates[0].findAll('td')[1].text.split("\t").pop().strip()
    return lastUpdate

def getImgName(soup):
    img = soup.find('img')
    if img:
        return img['src'].split('/').pop()
    else:
        return None

def getDFviaSoup(soup,categ):
    '''categ is used to get the business category name only.
    business soup structure:
    treeViewer.getIx(soup, [2,3,1,15,3]).findAll('table') has 4 entries:
    1. topic name
    2. what we want: entry content
    3. number of categories
    4. last update

    '''
    # set default category info for HotQuote, since that alone does not show category info
    catName="HotQuote"
    catText="ActivityHeat"
    if categ not in ETNET_TYPES[:2]:
        d1=soup.findAll('td', attrs={'class':'HeaderTxt ThemeColor'})
        if len(d1)!=1:
            print "category name may be incorrectly"
        catText = d1[0].text.strip()
        catName = categ
    # hot.php and business.php
    #<form name="sectorform" id="sectorform" method="post" action="/www/tc/stocks/sector_hot.php">
    #<table width="100%" border="0" cellspacing="0" cellpadding="2">
    t2= soup.find('form', attrs={'name':"sectorform", 'id':"sectorform"})
    if t2:
        table=t2.contents[1]
        page = t2['action'].split('/').pop()
    else:
        # hotquote:
        table=treeViewer.getIx(soup, [2,3,1,15,3,7,3,1])
        page = 'hotquote'

    if not table:
        print "cannot find table"
        return None
    
    lastUpdate = getLastUpdate(soup)

    df = pd.DataFrame(columns=['page','catName', 'catText','code','name','arrow','price','return','volume','mcap','currency','weekrate','yield','PErange'])

    debugCt = 1
    for t in table:
        if type(t) != bs.Tag:
            continue
        if t.name != 'tr':
            print 'unexpected element: not tr:' + t.name
            continue
        if t.has_key('class') and (t['class'][-3:]=='Row'):
            #get content: code, name, upArrow
            cols = t.findAll('td')
            r = {'catName':catName, 'catText':catText}
            r['code'] = cols.pop(0).text
            r['name'] = cols.pop(0).text.strip()
            r['arrow'] = getImgName(cols.pop(0))
            r['price'] = cols.pop(0).text
            r['return'] = cols.pop(0).text
            r['volume'] = cols.pop(0).text
            r['mcap'] = cols.pop(0).text
            r['currency'] = cols.pop(0).text
            r['weekrate'] = cols.pop(0).text
            r['yield'] = cols.pop(0).text
            r['PErange'] = getImgName(cols.pop(0))

            df = df.append(r, ignore_index = True)
        elif t.find('table'):
            cat=t.find('a')
            catName = cat['name']
            catText = t.find('table').text.strip()
        else:
            #print "unexpected element {}:".format(debugCt)
            #print t
            pass
        debugCt+=1

    df['lastUpdate']=lastUpdate
    df['page']=page
    return df

def getSoupFromWeb(categ):
    if categ in ETNET_TYPES:
        categStr = categ + '.php'
    else:
        categStr = 'business.php?business={}'.format(categ)
    url = 'http://www.etnet.com.hk/www/tc/stocks/sector_{}'.format(categStr)
    req = requests.get(url)
    common.pause(1)
    soup= bs.BeautifulSoup(req.content)
    return soup

def runConvert(date,altFlag=False):
    '''Example:
cd /home/wzhu/gitlabs/data-collection/etnet.hk
from etnet import *

#for date in pd.DateRange(dd(2015,1,20), dd(2015,3,23), offset=pd.datetools.day):
#    runConvert(date)

AltFlag=True
for date in pd.DateRange(dd(2015,1,20), dd(2015,1,23), offset=pd.datetools.day):
    runConvert(date, AltFlag)

    '''
    ymd = "{:%Y%m%d}".format(date)
    altDir = ymd+(".0" if altFlag else "")
    outdir = '/home/wzhu/download_daily/www.etnet.com.hk/processed/{}/'.format(altDir)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    for categ in ETNET_TYPES + range(1,DEFAULT_CAT_NUM+1):
        file = ymd + ".{}".format(categ)

        soup = getSoupFromFile(date,categ,altFlag)
        if not soup:
            print "no file for {} Q{}".format(date,categ)
            continue
        df = getDFviaSoup(soup,categ)
        if not df:
            print "empty DF for {} Q{}".format(date,categ)
            continue    
        df['date']=ymd
        df['qtype']=categ
        df.to_csv(outdir+file, encoding='utf8', index=False)

def run(date):
    liveFlag = True
    ymd = "{:%Y%m%d}".format(date)
    outdir = '/home/wzhu/download_daily/www.etnet.com.hk/live/{}/'.format(ymd)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    #get cat num
    soup = getSoupFromWeb(ETNET_TYPES[2])
    catNum = getCategoryNum(soup)

    for categ in ETNET_TYPES + range(1,catNum+1):
        #for testing hours of the day:
        file = ymd + ".{}.{:%H%M}".format(categ,date)

        soup = getSoupFromWeb(categ)
        time.sleep(2)
        df = getDFviaSoup(soup,categ)
        if not df:
            with open(outdir+file+'.pickle','wb') as fh:
                pickle.dump(soup, fh)
            continue

        df['date']=ymd
        df['qtype']=categ
        df.to_csv(outdir+file, encoding='utf8', index=False)
        
if __name__ == "__main__":
    run(dd.today())
