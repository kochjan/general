#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import sys
import time
import os


s = requests.Session()

url = 'http://www.borsaistanbul.com/en/data/data/equity-market-data/equity-based-data'
req = s.get(url)
soup= bs.BeautifulSoup(req.content)

viewstate =soup.find(type='hidden', id='__VIEWSTATE').get('value')
eventVal =soup.find(type='hidden', id='__EVENTVALIDATION').get('value')
ctl17 =soup.find(type='hidden', id='ctl17_TSM').get('value')
ctl18 =soup.find(type='hidden', id='ctl18_TSSM').get('value')

headers = {'User-Agent': 'Mozilla/5.0', 
           'Connection': 'keep-alive',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Accept-Encoding': 'gzip, deflate',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
           }


payload_base = {'__EVENTTARGET':'', '__EVENTARGUMENT':'', 
    '__VIEWSTATE': viewstate,
    '__EVENTVALIDATION':eventVal,
    'ctl00$ctl17':'',
    'ctl17_TSM':ctl17,
    'ctl18_TSSM':ctl18
}
# these are weekly downloads
payloadMap={
    'namecode': {
        'ctl00$TextContent$C001$lbtnHisseAdiKoduDegisiklikleri.x':'7',
	'ctl00$TextContent$C001$lbtnHisseAdiKoduDegisiklikleri.y':'10'},
    'foreign': {
        'ctl00$TextContent$C001$lbtnYabanciBankaAraciKurum.x':'2',
	'ctl00$TextContent$C001$lbtnYabanciBankaAraciKurum.y':'5'},
    'maxlot': {
        'ctl00$TextContent$C001$lbtnMaksimumLotMiktarlari.x':'11',
	'ctl00$TextContent$C001$lbtnMaksimumLotMiktarlari.y':'8'},
    'shortsales': {
        'ctl00$TextContent$C001$lbtnAcigaSatisIslemleri.x':'5',
        'ctl00$TextContent$C001$lbtnAcigaSatisIslemleri.y':'7'},
    'top20': {
        'ctl00$TextContent$C001$lbtnEnAktif20HisseVeUye.x':'8',
	'ctl00$TextContent$C001$lbtnEnAktif20HisseVeUye.y':'11'}
}
def getLoad(optionMap):
    load = dict(payload_base)
    load.update(optionMap)
    return load

outdir = './outputs/borsa.short'

def downloadBorsa(date):
    for key in payloadMap.keys():
        req2 = s.post(url,headers=headers,data=getLoad(payloadMap[key]))
        res = req2.headers.get('content-disposition')
	if pd.isnull(res):
	    print "no data on {}".format(date)
	elif 'filename' in res:
	    filename = res.split('=')[1]
	    print filename
	    filepath = '{}/{:%Y%m%d}.{}'.format(outdir, date, filename)
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        with open(filepath, "wb") as f:
            f.write(req2.content)
	# pause 5 secs
	time.sleep(5)


if __name__ == '__main__':
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    downloadBorsa(day)
    # these are weekly, so download previous Friday

