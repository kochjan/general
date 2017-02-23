#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import random
import sys
sys.path.insert(0,'/home/wzhu/gitlabs/data-collection/utils')
from common import pause

s = requests.Session()

url = 'http://www.borsaistanbul.com/en/data/data/equity-market-data'
req = s.request('GET', url, allow_redirects=True)
soup= bs.BeautifulSoup(req.content)

viewstate =soup.find(type='hidden', id='__VIEWSTATE').get('value')
eventVal =soup.find(type='hidden', id='__EVENTVALIDATION').get('value')
ctl17 =soup.find(type='hidden', id='ctl17_TSM').get('value')
ctl18 =soup.find(type='hidden', id='ctl18_TSSM').get('value')

headers = {'User-Agent': 'Mozilla/5.0', 
           'Connection': 'keep-alive',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
           }


payload_base = {'__EVENTTARGET':'', '__EVENTARGUMENT':'', 
    '__VIEWSTATE': viewstate,
    '__EVENTVALIDATION':eventVal,
    'ctl00$ctl17':'',
    'ctl17_TSM':ctl17,
    'ctl18_TSSM':ctl18,
    'ctl00$arama$T1DEB820E009$ctl00$ctl00$searchTextBox':''
}

def getLoad(date,session):
    load = dict(payload_base)
    load.update( {
	'ctl00$TextContent$C001$cboHisseSenetleriPiyasasi':session,
	'ctl00$TextContent$C001$btnHisseSenetleriPiyasasi.x': random.randint(0,12),
	'ctl00$TextContent$C001$btnHisseSenetleriPiyasasi.y': random.randint(0,13),
	'ctl00$TextContent$C001$rdpHisseSenetleriPiyasasi': '{:%Y-%m-%d}'.format(date),
	'ctl00$TextContent$C001$rdpHisseSenetleriPiyasasi$dateInput':'{:%-m/%-d/%Y}'.format(date)
    } )
    return load

import os

def downloadBorsaYear(year):
    for date in pd.DateRange(dd(year,1,1), dd(year,12,31), pd.datetools.BDay ):
	downloadBorsa(date)

def downloadBorsa(date):
    outdir = '/home/wzhu/download_daily/borsa.bulletin/{:%Y}'.format(date)
    if not os.path.exists(outdir):
	os.makedirs(outdir)
    print date

    for sess in [1,2]:
        req2 = s.post(url,headers=headers,data=getLoad(date,sess))
	pause() #5 sec
        res = req2.headers.get('content-disposition')
        if pd.isnull(res):
	    print "no data on {}".format(date)
	    break
        elif 'filename' in res:
	    filename = res.split('=')[1]
            print filename
	    filepath = '{}/{}'.format(outdir, filename)
            with open(filepath, "wb") as f:
	        f.write(req2.content)


if __name__ == '__main__':
    production = '''
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    downloadBorsa(day)
    '''
    for date in pd.DateRange(dd(2015,7,21), dd(2015,7,31), pd.datetools.BDay ):
	downloadBorsa(date)

