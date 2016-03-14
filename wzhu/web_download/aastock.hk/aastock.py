#!/opt/python2.7/bin/python
import pandas as pd
from datetime import datetime as dd
import pickle
import requests
import BeautifulSoup as bs
import re
import sys
sys.path.insert(0,'/home/wzhu/gitlabs/data-collection/utils')
import treeViewer

def getSoupFromFile(date,type, altFlag=False):
    dateStr = "{:%Y%m%d}".format(date)
    if altFlag: #test date.0 at other times
        dateStr += ".0"
    file = "/home/wzhu/download_daily/www.aastocks.com/{}/en%LTP/RTAI.aspx?type={}".format(dateStr, type)
    return pickle.load(open(file, 'r'))

def getLastUpdate(soup):
    c1=soup.findAll('span', attrs={'id':'ctl00_ctl00_cphContent_cphContent_lblLastUpdate'})
    if (len(c1)!=1):
        print "unexpected tag structure: {}!=1".format(len(c1))
        return None
    return c1[0].text.replace('Last Update: ','')


def getContentsNode(soup):
    c1=soup.findAll('div', attrs={'id':'ctl00_ctl00_cphContent_cphContent_divContent'})
    if (len(c1)!=1):
        print "unexpected tag structure: {}!=1".format(len(c1))
        return None
    return treeViewer.getIx(c1[0],[1,7,0,1])

def getContentsDF(soup, liveFlag):
    '''liveFlag demands everything as expected, else return None to signal saving the raw file'''
    df = pd.DataFrame(columns=['imgid','imgsrc','type','symbol','model_id','run_id','text']) 
    catNum = len(soup)
    if (catNum%4 != 3):
        print "warning: category index assumptions wrong: expect 3+4*CatNum, seeing ".format(catNum)
        if liveFlag:
            return None
    catNum = (catNum-3)/4 +1
    for iCt in range(catNum):
        itemContents = treeViewer.getIx(soup, [4*iCt +1, 0,1,1])
        if not itemContents:
            print "no itemContents at category {}".format(iCt)
            return None if liveFlag else df
        categoryNode = itemContents.contents[1]
        img = categoryNode.findAll('img', attrs={'id':re.compile("img[0-9]"), 'src':re.compile(".*/ai/EN.*")})
        if not img:
            print "no img at category {}".format(iCt)
            return None if liveFlag else df
        if len(img)!=1:
            print "more than 1 img: {}".format(len(img))
            if liveFlag:
                return None
        img0 = img[0]
        imgid = img0.get('id')
        imgsrc = img0.get('src').split('/').pop()

        contentNode = itemContents.contents[3]        
        if not contentNode:
            print "no contents at category {}".format(iCt)
            continue;

        for entry in contentNode.findAll('td', attrs={'class':'boxM'}):
            result = {}
            result['imgid']=imgid
            result['imgsrc']=imgsrc
            fields = entry.find('a').get('href').split('?')[1]
            # u'type=1&symbol=00097&model_id=3&run_id=1'
            for tok in fields.split('&'):
                (t,v) = tok.split('=')
                result[t]=v
            # u'HENDERSON...00097.HK'
            result['text']=entry.text
            df = df.append(result, ignore_index = True)
    return df

def getSoupFromWeb(date, type):
    if (not type in [1,2]):
        print "type not in [1,2]: {}".format(type)
        return None
    url = 'http://www.aastocks.com/en/LTP/RTAI.aspx?type={}'.format(type)
    req = requests.get(url)
    soup= bs.BeautifulSoup(req.content)
    return soup

def runConvert(date,altFlag=False):
    '''Example:
cd /home/wzhu/gitlabs/data-collection/aastock.hk
from aastock import *
for date in pd.DateRange(dd(2015,1,20), dd(2015,3,23), offset=pd.datetools.day):
    runConvert(date)

AltFlag=True
for date in pd.DateRange(dd(2015,1,22), dd(2015,1,23), offset=pd.datetools.day):
    runConvert(date, AltFlag)
    '''
    outdir = '/home/wzhu/download_daily/www.aastocks.com/processed/'
    historyConvert = False
    liveFlag = True
    for type in [1,2]:
        soup = getSoupFromFile(date,type,altFlag)
        lastUpdate = getLastUpdate(soup)
        content = getContentsNode(soup)
        df = getContentsDF(content, historyConvert)
        df['date']="{:%Y%m%d}".format(date)
        df['qtype']=type
        df['lastUpdate']=lastUpdate
        outfile = outdir+"{:%Y%m%d}.{}".format(date,type)+(".0" if altFlag else "")
        df.to_csv(outfile, encoding='utf8')

def run(date):
    liveFlag = True
    outdir = '/home/wzhu/download_daily/www.aastocks.com/live/'
    for type in [1,2]:
        ymd = "{:%Y%m%d}".format(date)    
        #file = ymd + ".{}".format(type)
        #for testing hours of the day:
        file = ymd + ".{}.{:%H%M}".format(type,date)

        soup = getSoupFromWeb(date,type)
        lastUpdate = getLastUpdate(soup)
        content = getContentsNode(soup)
        if not content:
            with open(outdir+file+'.pickle','wb') as fh:
                pickle.dump(soup, fh)
            continue

        df = getContentsDF(content, liveFlag)
        if not df:
            with open(outdir+file+'.pickle','wb') as fh:
                pickle.dump(soup, fh)
            continue
        df['date']=ymd
        df['qtype']=type
        df['lastUpdate']=lastUpdate
        df.to_csv(outdir+file, encoding='utf8')

if __name__ == "__main__":
    run(dd.today())
