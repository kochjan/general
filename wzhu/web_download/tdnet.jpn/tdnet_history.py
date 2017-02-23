#!/opt/python2.7/bin/python
'''
This module downloads from TDnet: first login, then queries, and download every result, then logout.
While we can query all, we prefer to focus on the important Earning Announcement query.
Example usage:
1. download EA for single day: just run this script (main() executes 'run(today)')
2. download a series of days: 
    -use iPython to avoid multiple login:
%cd /home/wzhu/gitlabs/data-collection/tdnet.jpn
from tdnet_history import *
init()
enterJapan()
enterTab()
allDoc = True
keywordQ= False
for rdate in pd.DateRange(dd(2010,7,1), dd(2011,1,1), offset=pd.datetools.day):
    runGeneral(rdate, keywordQ)
    time.sleep(3)
logout()

'''

import requests
import BeautifulSoup as bs
import pandas as pd
import os
# for sleep:
import time
import random
# for debug output
import pickle
# for patchMode csv
import codecs
from datetime import datetime as dd

INFO = True
# TODO: use logging instead

DEFAULT_OUTDIR_BASE = '/home/wzhu/download_daily/tdnet'

# init
s = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0'}

homeurl='https://www.tmi.tse.or.jp/'

# The page to click on Japan/English
page2 = homeurl + 'tmtwb/CMN010010Action.do'
show = '?Show=Show'

# The page to click TDnetDBS tab
page3 = homeurl + "tmtwb/CMN010900Action.do"
tab = '?tdnetDbsTab=tdnetDbsTab'

# The page to send query
page4 = homeurl + "tmtwb/TDN010010Action.do"

def soup(req):
    return bs.BeautifulSoup(req.content)

def pause(sec=5):
    r = 0
    while (r < 0.3):
        r = random.normalvariate(sec,0.5)
        sec +=1
    time.sleep(r)
    return

def init():
    ''' get credentials '''
    # Initial homepage
    link = homeurl
    req1= s.get(link, headers=headers)
    soup1 = soup(req1)
    
    # Login with credentials (via redirects)
    inputs=soup1.find('form').findAll('input')
    payload= {}
    for tag in inputs:
        payload[tag.get('name')] = tag.get('value')
    
    payload['ACCOUNTUID']='US000015H3'
    payload['PASSWORD']='qLDlaMcG1'
    
    headers['Referer']= link
    link    = homeurl+'fw/dfw'
    req2    = s.post(link,headers=headers,data=payload,allow_redirects=True)
    
    if (homeurl+'iwcd/cdtop/tgkbn/' != req2.history[0].headers['location']):
        print "Something wrong"
    
    # Processing login request, please wait.
    link    = 'https://www.tmi.tse.or.jp/'
    headers['Referer']= homeurl+'iwcd/cdtop/tgkbn/'
    req3    = s.get(link,headers=headers)
    
    pause(1)
    return req3

def enterJapan():
    page2show = page2 + '?Show=Show'
    headers['Referer']= homeurl
    link = page2show
    req4 = s.get(link,headers=headers)
    soup4= bs.BeautifulSoup(req4.content)
    
    # showing Japanese vs English. Click Japanese
    headers['Referer']= link
    link    = page2
    payload={'Login':'Login'}
    inputs2 = soup4.find('dl').findAll('input')
    for tag in inputs2:
        payload[tag.get('name')] = tag.get('value')
    
    req5    = s.post(link,headers=headers,data=payload)
    pause(1)
    return req5

def enterTab(reqFlag=False):
    # need to explicitly setting cookies 
    # run this after enterJapan()
    # set a flag that returns the request instead of status
    link    = page3 + tab
    headers['Referer']= page2
    reqA = s.get(link,headers=headers,cookies=s.cookies)
    pause(2)
    
    if len(soup(reqA).findAll('input'))>100:
        print "refresh ok"
        rc = True
    else:
        print "should check soup(_)"
        rc = False
    
    if reqFlag:
        return reqA
    else:
        return rc

x=unicode(u'\u696D\u7E3E\u4E88\u60F3\u914D\u5F53')

hstr='''Connection: keep-alive
Cache-Control: max-age=0
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Content-Type: application/x-www-form-urlencoded
Accept-Encoding: gzip, deflate
Accept-Language: en-US,en;q=0.8'''
header2 = {'User-Agent': 'Mozilla/5.0'}
for p in hstr.split("\n"):
    (a,b)=p.split(':')
    header2[a]=b

def enterQuery(query):
    link = page4
    header2['Referer'] = page3 + tab
    #header2['Content-Type'] = 'application/x-www-form-urlencoded; charset=utf8'
    reqB = s.post(link, headers=header2, data=query, cookies=s.cookies) #, allow_redirects=True)
    pause(2)
    return reqB

# Logout
def logout():
    link = page3
    headers['Referer'] = page3
    payload={'Logout':'Logout'}
    reqLO  = s.post(link,headers=headers,data=payload)
    return reqLO



def download_file(url, local_filename):
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    print "Done"



# next is the raw form data for 2010/12/13 14:15 - 2010/12/14 16:18 all stock
# FormSourceAllStockPart2='''longstring'''
FormSourceDatePart1='ibntKbn=&itemCdNo=&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknFrmYear={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknFrmTsk={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknFrmDay={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknFrmJ={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknFrmFn={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknToYear={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknToTsk={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknToDay={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknToJ={}&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknToFn={}'

FormSourceAllStockPart2 = '&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknRdobtn=1&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd1=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd2=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd3=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm1=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm2=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm3=&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi1=&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi2=&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi3=&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=00&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=11&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=12&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=13&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=24&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=25&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=36&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=37&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk1TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk2TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk3TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.shhnKbnChkbxLst=01&ccTdn010010GmnDspNiyLst_st%5B0%5D.shhnKbnChkbxLstMapOut=00%3E%E5%85%A8%E3%81%A6%3C01%3E%E5%86%85%E5%9B%BD%E4%BC%9A%E7%A4%BE%3C0A%3E%E5%86%85%E5%9B%BD%EF%BC%88%E5%84%AA%E5%85%88%EF%BC%89%E5%87%BA%E8%B3%87%E8%A8%BC%E5%88%B8%3C11%3E%E5%A4%96%E5%9B%BD%E4%BC%9A%E7%A4%BE%3CB1%3E%E5%86%85%E5%9B%BDETF%3CB2%3E%E5%86%85%E5%9B%BDREIT%3CB3%3E%E5%A4%96%E5%9B%BDETF%E3%83%BBETN%3CB4%3E%E5%A4%96%E5%9B%BDREIT%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=000000&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010101&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010102&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010104&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010106&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010105&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010109&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020201&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020202&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020204&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020209&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030301&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030302&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030303&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030309&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080801&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080802&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080809&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060601&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060602&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060609&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=101001&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=101002&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=111101&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLstMapOut=000000%3E%E5%85%A8%E3%81%A6%3C010101%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C010102%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C010104%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%83%9E%E3%82%B6%E3%83%BC%E3%82%BA%EF%BC%89%3C010106%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88JASDAQ%EF%BC%89%3C010105%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88TOKYO+PRO+Market%EF%BC%89%3C010109%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C020201%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C020202%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C020204%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88JASDAQ%EF%BC%89%3C020209%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C030301%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C030302%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C030303%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%82%BB%E3%83%B3%E3%83%88%E3%83%AC%E3%83%83%E3%82%AF%E3%82%B9%EF%BC%89%3C030309%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C080801%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C080802%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%82%A2%E3%83%B3%E3%83%93%E3%82%B7%E3%83%A3%E3%82%B9%EF%BC%89%3C080809%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C060601%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C060602%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88Q-Board%EF%BC%89%3C060609%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C101001%3E%E3%82%B8%E3%83%A3%E3%82%B9%E3%83%80%E3%83%83%E3%82%AF%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C101002%3E%E3%82%B8%E3%83%A3%E3%82%B9%E3%83%80%E3%83%83%E3%82%AF%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88NEO%EF%BC%89%3C111101%3E%E6%97%A5%E6%9C%AC%E8%A8%BC%E5%88%B8%E6%A5%AD%E5%8D%94%E4%BC%9A%EF%BC%88%E3%82%B0%E3%83%AA%E3%83%BC%E3%83%B3%E3%82%B7%E3%83%BC%E3%83%88%E7%AD%89%EF%BC%89%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=0000&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=0050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=1050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=2050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3250&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3300&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3350&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3400&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3450&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3500&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3550&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3600&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3650&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3700&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3750&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3800&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=4050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5250&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=6050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=6100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=8050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=9050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=9999&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLstMapOut=0000%3E%E5%85%A8%E3%81%A6%3C0050%3E%E6%B0%B4%E7%94%A3%E3%83%BB%E8%BE%B2%E6%9E%97%E6%A5%AD%3C1050%3E%E9%89%B1%E6%A5%AD%3C2050%3E%E5%BB%BA%E8%A8%AD%E6%A5%AD%3C3050%3E%E9%A3%9F%E6%96%99%E5%93%81%3C3100%3E%E7%B9%8A%E7%B6%AD%E8%A3%BD%E5%93%81%3C3150%3E%E3%83%91%E3%83%AB%E3%83%97%E3%83%BB%E7%B4%99%3C3200%3E%E5%8C%96%E5%AD%A6%3C3250%3E%E5%8C%BB%E8%96%AC%E5%93%81%3C3300%3E%E7%9F%B3%E6%B2%B9%E3%83%BB%E7%9F%B3%E7%82%AD%E8%A3%BD%E5%93%81%3C3350%3E%E3%82%B4%E3%83%A0%E8%A3%BD%E5%93%81%3C3400%3E%E3%82%AC%E3%83%A9%E3%82%B9%E3%83%BB%E5%9C%9F%E7%9F%B3%E8%A3%BD%E5%93%81%3C3450%3E%E9%89%84%E9%8B%BC%3C3500%3E%E9%9D%9E%E9%89%84%E9%87%91%E5%B1%9E%3C3550%3E%E9%87%91%E5%B1%9E%E8%A3%BD%E5%93%81%3C3600%3E%E6%A9%9F%E6%A2%B0%3C3650%3E%E9%9B%BB%E6%B0%97%E6%A9%9F%E5%99%A8%3C3700%3E%E8%BC%B8%E9%80%81%E7%94%A8%E6%A9%9F%E5%99%A8%3C3750%3E%E7%B2%BE%E5%AF%86%E6%A9%9F%E5%99%A8%3C3800%3E%E3%81%9D%E3%81%AE%E4%BB%96%E8%A3%BD%E5%93%81%3C4050%3E%E9%9B%BB%E6%B0%97%E3%83%BB%E3%82%AC%E3%82%B9%E6%A5%AD%3C5050%3E%E9%99%B8%E9%81%8B%E6%A5%AD%3C5100%3E%E6%B5%B7%E9%81%8B%E6%A5%AD%3C5150%3E%E7%A9%BA%E9%81%8B%E6%A5%AD%3C5200%3E%E5%80%89%E5%BA%AB%E3%83%BB%E9%81%8B%E8%BC%B8%E9%96%A2%E9%80%A3%E6%A5%AD%3C5250%3E%E6%83%85%E5%A0%B1%E3%83%BB%E9%80%9A%E4%BF%A1%E6%A5%AD%3C6050%3E%E5%8D%B8%E5%A3%B2%E6%A5%AD%3C6100%3E%E5%B0%8F%E5%A3%B2%E6%A5%AD%3C7050%3E%E9%8A%80%E8%A1%8C%E6%A5%AD%3C7100%3E%E8%A8%BC%E5%88%B8%E3%80%81%E5%95%86%E5%93%81%E5%85%88%E7%89%A9%E5%8F%96%E5%BC%95%E6%A5%AD%3C7150%3E%E4%BF%9D%E9%99%BA%E6%A5%AD%3C7200%3E%E3%81%9D%E3%81%AE%E4%BB%96%E9%87%91%E8%9E%8D%E6%A5%AD%3C8050%3E%E4%B8%8D%E5%8B%95%E7%94%A3%E6%A5%AD%3C9050%3E%E3%82%B5%E3%83%BC%E3%83%93%E3%82%B9%E6%A5%AD%3C9999%3E%E3%81%9D%E3%81%AE%E4%BB%96%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.dwnladSryoUmRdobtn=1&ListShow=ListShow'



# QUERY SOURCE for 2011/1/2 03:04 - 2011/1/3 05:07 3keyword
# FormSource3Keyword='''longstring'''


FormSource3KeywordPart2='&ccTdn010010GmnDspNiyLst_st%5B0%5D.kiknRdobtn=1&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd1=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd2=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrCd3=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm1=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm2=&ccTdn010010GmnDspNiyLst_st%5B0%5D.mgrNm3=&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi1=%E6%A5%AD%E7%B8%BE&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi2=%E4%BA%88%E6%83%B3&ccTdn010010GmnDspNiyLst_st%5B0%5D.hydi3=%E9%85%8D%E5%BD%93&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=00&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=11&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=12&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=13&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=24&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=25&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=36&ccTdn010010GmnDspNiyLst_st%5B0%5D.shtiKbn1ChkbxLst=37&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk1TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk2TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.kkiKmk3TxtBx=&ccTdn010010GmnDspNiyLst_st%5B0%5D.shhnKbnChkbxLst=01&ccTdn010010GmnDspNiyLst_st%5B0%5D.shhnKbnChkbxLstMapOut=00%3E%E5%85%A8%E3%81%A6%3C01%3E%E5%86%85%E5%9B%BD%E4%BC%9A%E7%A4%BE%3C0A%3E%E5%86%85%E5%9B%BD%EF%BC%88%E5%84%AA%E5%85%88%EF%BC%89%E5%87%BA%E8%B3%87%E8%A8%BC%E5%88%B8%3C11%3E%E5%A4%96%E5%9B%BD%E4%BC%9A%E7%A4%BE%3CB1%3E%E5%86%85%E5%9B%BDETF%3CB2%3E%E5%86%85%E5%9B%BDREIT%3CB3%3E%E5%A4%96%E5%9B%BDETF%E3%83%BBETN%3CB4%3E%E5%A4%96%E5%9B%BDREIT%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=000000&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010101&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010102&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010104&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010106&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010105&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=010109&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020201&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020202&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020204&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=020209&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030301&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030302&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030303&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=030309&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080801&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080802&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=080809&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060601&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060602&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=060609&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=101001&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=101002&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLst=111101&ccTdn010010GmnDspNiyLst_st%5B0%5D.sjKbnChkbxLstMapOut=000000%3E%E5%85%A8%E3%81%A6%3C010101%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C010102%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C010104%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%83%9E%E3%82%B6%E3%83%BC%E3%82%BA%EF%BC%89%3C010106%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88JASDAQ%EF%BC%89%3C010105%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88TOKYO+PRO+Market%EF%BC%89%3C010109%3E%E6%9D%B1%E4%BA%AC%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C020201%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C020202%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C020204%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88JASDAQ%EF%BC%89%3C020209%3E%E5%A4%A7%E9%98%AA%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C030301%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%80%E9%83%A8%EF%BC%89%3C030302%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%BA%8C%E9%83%A8%EF%BC%89%3C030303%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%82%BB%E3%83%B3%E3%83%88%E3%83%AC%E3%83%83%E3%82%AF%E3%82%B9%EF%BC%89%3C030309%3E%E5%90%8D%E5%8F%A4%E5%B1%8B%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C080801%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C080802%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%82%A2%E3%83%B3%E3%83%93%E3%82%B7%E3%83%A3%E3%82%B9%EF%BC%89%3C080809%3E%E6%9C%AD%E5%B9%8C%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C060601%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C060602%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88Q-Board%EF%BC%89%3C060609%3E%E7%A6%8F%E5%B2%A1%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E3%81%9D%E3%81%AE%E4%BB%96%EF%BC%89%3C101001%3E%E3%82%B8%E3%83%A3%E3%82%B9%E3%83%80%E3%83%83%E3%82%AF%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88%E4%B8%8A%E5%A0%B4%EF%BC%89%3C101002%3E%E3%82%B8%E3%83%A3%E3%82%B9%E3%83%80%E3%83%83%E3%82%AF%E8%A8%BC%E5%88%B8%E5%8F%96%E5%BC%95%E6%89%80%EF%BC%88NEO%EF%BC%89%3C111101%3E%E6%97%A5%E6%9C%AC%E8%A8%BC%E5%88%B8%E6%A5%AD%E5%8D%94%E4%BC%9A%EF%BC%88%E3%82%B0%E3%83%AA%E3%83%BC%E3%83%B3%E3%82%B7%E3%83%BC%E3%83%88%E7%AD%89%EF%BC%89%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=0000&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=0050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=1050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=2050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3250&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3300&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3350&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3400&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3450&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3500&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3550&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3600&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3650&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3700&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3750&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=3800&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=4050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=5250&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=6050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=6100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7100&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7150&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=7200&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=8050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=9050&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=9999&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLstMapOut=0000%3E%E5%85%A8%E3%81%A6%3C0050%3E%E6%B0%B4%E7%94%A3%E3%83%BB%E8%BE%B2%E6%9E%97%E6%A5%AD%3C1050%3E%E9%89%B1%E6%A5%AD%3C2050%3E%E5%BB%BA%E8%A8%AD%E6%A5%AD%3C3050%3E%E9%A3%9F%E6%96%99%E5%93%81%3C3100%3E%E7%B9%8A%E7%B6%AD%E8%A3%BD%E5%93%81%3C3150%3E%E3%83%91%E3%83%AB%E3%83%97%E3%83%BB%E7%B4%99%3C3200%3E%E5%8C%96%E5%AD%A6%3C3250%3E%E5%8C%BB%E8%96%AC%E5%93%81%3C3300%3E%E7%9F%B3%E6%B2%B9%E3%83%BB%E7%9F%B3%E7%82%AD%E8%A3%BD%E5%93%81%3C3350%3E%E3%82%B4%E3%83%A0%E8%A3%BD%E5%93%81%3C3400%3E%E3%82%AC%E3%83%A9%E3%82%B9%E3%83%BB%E5%9C%9F%E7%9F%B3%E8%A3%BD%E5%93%81%3C3450%3E%E9%89%84%E9%8B%BC%3C3500%3E%E9%9D%9E%E9%89%84%E9%87%91%E5%B1%9E%3C3550%3E%E9%87%91%E5%B1%9E%E8%A3%BD%E5%93%81%3C3600%3E%E6%A9%9F%E6%A2%B0%3C3650%3E%E9%9B%BB%E6%B0%97%E6%A9%9F%E5%99%A8%3C3700%3E%E8%BC%B8%E9%80%81%E7%94%A8%E6%A9%9F%E5%99%A8%3C3750%3E%E7%B2%BE%E5%AF%86%E6%A9%9F%E5%99%A8%3C3800%3E%E3%81%9D%E3%81%AE%E4%BB%96%E8%A3%BD%E5%93%81%3C4050%3E%E9%9B%BB%E6%B0%97%E3%83%BB%E3%82%AC%E3%82%B9%E6%A5%AD%3C5050%3E%E9%99%B8%E9%81%8B%E6%A5%AD%3C5100%3E%E6%B5%B7%E9%81%8B%E6%A5%AD%3C5150%3E%E7%A9%BA%E9%81%8B%E6%A5%AD%3C5200%3E%E5%80%89%E5%BA%AB%E3%83%BB%E9%81%8B%E8%BC%B8%E9%96%A2%E9%80%A3%E6%A5%AD%3C5250%3E%E6%83%85%E5%A0%B1%E3%83%BB%E9%80%9A%E4%BF%A1%E6%A5%AD%3C6050%3E%E5%8D%B8%E5%A3%B2%E6%A5%AD%3C6100%3E%E5%B0%8F%E5%A3%B2%E6%A5%AD%3C7050%3E%E9%8A%80%E8%A1%8C%E6%A5%AD%3C7100%3E%E8%A8%BC%E5%88%B8%E3%80%81%E5%95%86%E5%93%81%E5%85%88%E7%89%A9%E5%8F%96%E5%BC%95%E6%A5%AD%3C7150%3E%E4%BF%9D%E9%99%BA%E6%A5%AD%3C7200%3E%E3%81%9D%E3%81%AE%E4%BB%96%E9%87%91%E8%9E%8D%E6%A5%AD%3C8050%3E%E4%B8%8D%E5%8B%95%E7%94%A3%E6%A5%AD%3C9050%3E%E3%82%B5%E3%83%BC%E3%83%93%E3%82%B9%E6%A5%AD%3C9999%3E%E3%81%9D%E3%81%AE%E4%BB%96%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLst=9999&ccTdn010010GmnDspNiyLst_st%5B0%5D.gyshKbnChkbxLstMapOut=0000%3E%E5%85%A8%E3%81%A6%3C0050%3E%E6%B0%B4%E7%94%A3%E3%83%BB%E8%BE%B2%E6%9E%97%E6%A5%AD%3C1050%3E%E9%89%B1%E6%A5%AD%3C2050%3E%E5%BB%BA%E8%A8%AD%E6%A5%AD%3C3050%3E%E9%A3%9F%E6%96%99%E5%93%81%3C3100%3E%E7%B9%8A%E7%B6%AD%E8%A3%BD%E5%93%81%3C3150%3E%E3%83%91%E3%83%AB%E3%83%97%E3%83%BB%E7%B4%99%3C3200%3E%E5%8C%96%E5%AD%A6%3C3250%3E%E5%8C%BB%E8%96%AC%E5%93%81%3C3300%3E%E7%9F%B3%E6%B2%B9%E3%83%BB%E7%9F%B3%E7%82%AD%E8%A3%BD%E5%93%81%3C3350%3E%E3%82%B4%E3%83%A0%E8%A3%BD%E5%93%81%3C3400%3E%E3%82%AC%E3%83%A9%E3%82%B9%E3%83%BB%E5%9C%9F%E7%9F%B3%E8%A3%BD%E5%93%81%3C3450%3E%E9%89%84%E9%8B%BC%3C3500%3E%E9%9D%9E%E9%89%84%E9%87%91%E5%B1%9E%3C3550%3E%E9%87%91%E5%B1%9E%E8%A3%BD%E5%93%81%3C3600%3E%E6%A9%9F%E6%A2%B0%3C3650%3E%E9%9B%BB%E6%B0%97%E6%A9%9F%E5%99%A8%3C3700%3E%E8%BC%B8%E9%80%81%E7%94%A8%E6%A9%9F%E5%99%A8%3C3750%3E%E7%B2%BE%E5%AF%86%E6%A9%9F%E5%99%A8%3C3800%3E%E3%81%9D%E3%81%AE%E4%BB%96%E8%A3%BD%E5%93%81%3C4050%3E%E9%9B%BB%E6%B0%97%E3%83%BB%E3%82%AC%E3%82%B9%E6%A5%AD%3C5050%3E%E9%99%B8%E9%81%8B%E6%A5%AD%3C5100%3E%E6%B5%B7%E9%81%8B%E6%A5%AD%3C5150%3E%E7%A9%BA%E9%81%8B%E6%A5%AD%3C5200%3E%E5%80%89%E5%BA%AB%E3%83%BB%E9%81%8B%E8%BC%B8%E9%96%A2%E9%80%A3%E6%A5%AD%3C5250%3E%E6%83%85%E5%A0%B1%E3%83%BB%E9%80%9A%E4%BF%A1%E6%A5%AD%3C6050%3E%E5%8D%B8%E5%A3%B2%E6%A5%AD%3C6100%3E%E5%B0%8F%E5%A3%B2%E6%A5%AD%3C7050%3E%E9%8A%80%E8%A1%8C%E6%A5%AD%3C7100%3E%E8%A8%BC%E5%88%B8%E3%80%81%E5%95%86%E5%93%81%E5%85%88%E7%89%A9%E5%8F%96%E5%BC%95%E6%A5%AD%3C7150%3E%E4%BF%9D%E9%99%BA%E6%A5%AD%3C7200%3E%E3%81%9D%E3%81%AE%E4%BB%96%E9%87%91%E8%9E%8D%E6%A5%AD%3C8050%3E%E4%B8%8D%E5%8B%95%E7%94%A3%E6%A5%AD%3C9050%3E%E3%82%B5%E3%83%BC%E3%83%93%E3%82%B9%E6%A5%AD%3C9999%3E%E3%81%9D%E3%81%AE%E4%BB%96%3C&ccTdn010010GmnDspNiyLst_st%5B0%5D.dwnladSryoUmRdobtn=1&ListShow=ListShow'

def getQuery(beginDate, endDate, alldocFlag):
    queryP1 = FormSourceDatePart1.format(beginDate.year, beginDate.month, beginDate.day, 0, 0, endDate.year, endDate.month, endDate.day, 0, 0)
    queryP2 = FormSourceAllStockPart2 if alldocFlag else FormSource3KeywordPart2
    return queryP1 + queryP2

def patchQuery(endDate, alldocFlag):
    queryP1 = FormSourceDatePart1.format(endDate.year, endDate.month, endDate.day, 0, 0, endDate.year, endDate.month, endDate.day, endDate.hour, endDate.minute)
    queryP2 = FormSourceAllStockPart2 if alldocFlag else FormSource3KeywordPart2
    return queryP1 + queryP2

pageA = homeurl + 'tmtwb/TDN010020Action.do'
def resultPageSet100():
    '''Make query result page show 100 per page'''
    link = pageA
    header2['Referer'] = page4
    qstring='dspGs=100&dspGs=20&LineChange=LineChange'
    req100 = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    pause(2)
    return req100


def resultGoFromToPage(Nfrom,Nto):
    '''On query result, goto 0-index page n from page n-1'''
    link = pageA
    header2['Referer'] = pageA
    qstring= 'dspGs=100&dspGs=100&dspHni={}&PageChange=PageChange&dspHni={}'.format(Nto, Nfrom)
    reqNextPage = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    pause(2)
    return reqNextPage

def resultGotoPage(n):
    return resultGoFromToPage(n-1, n)

def resultGoBackOneTo(n):
    return resultGoFromToPage(n+1, n)

def getMaxPage(soup):
    element = soup.find('input', attrs={'type':'hidden', 'name':'pageMax'})
    if element:
        return element.get('value')
    else:
        return 0

def getResultTag(soup):
    element = soup.find('table', attrs={'class':'results'})
    if element:
        return element.find('tbody')
    else:
        return None

# GEM = precious cargo: pdf or zip
GEMmark = 'content-disposition'

def getTitleLink(linkid, outdir):
    '''After query, download TitleLink associated pdf'''
    link = pageA
    header2['Referer'] = pageA
    qstring= 'tshShti={}&TitleLink=TitleLink'.format(linkid)
    reqPDF = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    pause(3)

    rh = reqPDF.headers
    if (rh[GEMmark] and len(rh[GEMmark].split('='))==2 and rh[GEMmark].endswith('pdf') ):
        fname = rh[GEMmark].split('=')[1].strip()
        with open(outdir+'/'+fname, 'wb') as file:
            file.write(reqPDF.content)
        return True
    else:
        print 'GetTitleLink error with outdir {}, linkid {}: header:'.format(outdir, linkid)
        print reqPDF.headers
        return False

def getXBRL(linkid, outdir):
    '''After query, download associated XBRL.zips'''
    link = pageA
    header2['Referer'] = pageA
    qstring= 'tshShti={}&Dwnlad=Dwnlad'.format(linkid)
    reqNewWindow1 = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    pause(1)

    link = homeurl + 'tmtwb/CMN010870Action.do'
    header2['Referer'] = pageA
    qstring= 'Show=Show'
    reqNewWindow2 = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    sNW2 = soup(reqNewWindow2)
    pause(2)

    header2['Referer'] = link
    kw = sNW2.find('form').find('input', attrs={'type':'submit'})
    kwmap = { kw.get('name') : kw.get('value') }
    reqXBRL = s.post(link, headers=header2, data= kwmap, cookies=s.cookies)
    pause(2)

    rh = reqXBRL.headers
    if (rh[GEMmark] and len(rh[GEMmark].split('='))==2 and rh[GEMmark].endswith('zip') ):
        fname = rh[GEMmark].split('=')[1].strip()
        with open(outdir+'/'+fname, 'wb') as file:
            file.write(reqXBRL.content)
        return True
    else:
        print 'GetXBRLLink error with outdir {}, linkid {}: header:'.format(outdir, linkid)
        print reqXBRL.headers
        return False


def crunchResultToDF(soup, df, allFlag, outdir):
    ''' 
    Process query page items. If KeywordQuery, will save all GEMS to outdir.
    Args:
    soup: should be the getResultTag(requestSoup)
    df: empty dataframe to be filled

    Remainder used for saving keyword query results:
    allFlag: True for all-doc-query, False for keyword-query
    outdir: final directory in which PDF and zip files are to be stored
    '''
    if (not allFlag):
        # should signal massive download
        if INFO:
            print "downloading {} PDF and some zips".format((len(soup.contents)-5)/2)

    for tag in soup.contents:
        if (type(tag) != bs.Tag): 
            # skips Comments and NavigatableStrings ('\n')
            continue
        
        hidden = tag.findAll('input', attrs={'type':'hidden'})
        # <input type="hidden" name="ccTdn010020LstDspNiy_st[0].mgrCd" value="94440" />
        if len(hidden) < 3:
            if INFO:
                print "skipping tag:", hidden
            continue
        
        result = {}
        
        for field in hidden:
            fullname = field.get('name')
            val = field.get('value')
            name = fullname.split('.')
            if len(name) != 2:
                print 'non-item info', hidden
                continue
            fname = name[1]
            result[fname] = val
            # check if we need to add new field_column to df
            if (not fname in df.columns):
                df[fname]=None
        
        df = df.append(result, ignore_index = True)

        # for Keyword Searches, save goodies
        if (not allFlag) and ('kijNo' in result): #signals keyword
            docid = result['kijNo']
            if 'hydiLnkPsCd' in result:
                # check if PDF file (with prefix=1401) already exist
                if (os.path.isfile("{}/1401{}.pdf".format(outdir,docid))):
                    if INFO:
                        print "{}.pdf exists".format(docid)
                else:
                    success= getTitleLink(result['hydiLnkPsCd'], outdir)
                    if INFO and not success:
                        print "Fail to download PDF in item"    
                        print result
            if 'dwnladPsCd' in result:
                # check if zip file (with prefix=0812 or 0912) already exist
                if (os.path.isfile("{}/0812{}.zip".format(outdir,docid)) or
                    os.path.isfile("{}/0912{}.zip".format(outdir,docid)) ):
                    if INFO:
                        print "{}.zip exists".format(docid)
                else:
                    success= getXBRL(result['dwnladPsCd'], outdir)
                    if INFO and not success:
                        print "Fail to download XBRL in item"    
                        print result
    return df

NORELEVANTDATA = unicode(u'\u8A72\u5F53\u30C7\u30FC\u30BF\u304C\u3042\u308A\u307E\u305B\u3093\u3067\u3057\u305F')

SEARCHAGAIN = unicode(u'\u8A72\u5F53\u30DA\u30FC\u30B8\u304C\u3042\u308A\u307E\u305B\u3093')

import glob

def runGeneral(date, alldocFlag, outdirbase=DEFAULT_OUTDIR_BASE, patchMode=False):
    '''
    This will do a historical query for a whole day, downloading associated document if applicable
    date: 
        the datetime date on which web items are announced
    alldocFlag: 
        true for all query (thus no download)
        false for earning announcement query
    outdirbase:
        destination base, in which we create yyyymm/ymd/querypage.csv; yyyymm/ymd/docid.pdf

    Optional:
    patchMode(=False):
        turn the code into patching the alldoc query which are too large on certain days

    Note:
    To run this in iPython, must have already ran:
    cd /home/wzhu/gitlabs/data-collection/tdnet.jpn/
    from TDstep import *
    init()
    enterJapan()
    enterTab()
    '''
    nextDay = date + pd.datetools.day
    query = getQuery(date, nextDay, alldocFlag)
    suboutdir = outdirbase + "/history"
    if alldocFlag:
        suboutdir = suboutdir + ".all"
    
    pathdir = suboutdir + "/{:%Y%m}/{:%Y%m%d}".format(date,date)
    if not os.path.exists(pathdir):
        os.makedirs(pathdir)
    csvFname = pathdir + "/queryResult.{:%Y%m%d}.csv".format(date)
    
    if patchMode:
        # patchMode only for existence of *null files
        nullFiles = glob.glob(csvFname + '*null')
        if not nullFiles:
            return
        else:
            # get ready for patching
            fname = nullFiles[0]
            fh = codecs.open(fname, 'r', encoding='utf8')
            lastEntryTimeStr = fh.readlines().pop().split(',')[1]
            # format = '2010/05/14 15:20'
            endDatetime = dd.strptime(lastEntryTimeStr, '%Y/%m/%d %H:%M')
            query = patchQuery(endDatetime, alldocFlag)
            os.rename(fname, fname+'.2') 
    # patchMode is independent of the following stanza
    if os.path.isfile(csvFname):
        # done if result file exists
        if INFO:
            print csvFname
        return
    else: # redo only for the case of: searchAgain, empty
        existingFiles = glob.glob(csvFname + '*')
        if any(map(lambda x: x.endswith('searchAgain') or x.endswith('empty'), existingFiles)):
            for file in existingFiles:
                os.rename(file, file+'.2')
                # rename the files to save them as indicators, 
                # as well as to end the potential runGeneral recursion
            return runGeneral(date, alldocFlag, outdirbase)
            # rerun with previous files moved
    
    #init()
    #enterJapan()
    #enterTab()
    if INFO:
        print "querying for {}".format(date)

    reqB1 = enterQuery(query)
    soupB1=soup(reqB1)
    if (not getResultTag(soupB1)):
        if (soupB1.text.find(NORELEVANTDATA) > -1):
            file = open(csvFname+".empty", "w")
            file.write('empty')
            file.close()
            if INFO:
                print "empty"
        elif (soupB1.text.find(SEARCHAGAIN) > -1):
            file = open(csvFname+".searchAgain", "w")
            file.write('data updated, no corresponding page')
            file.close()
            if INFO:
                print "search again"
        else:
            print "something wrong: cannot get query result. returning request for debugging"
            pickle.dump(soupB1, open(csvFname+".debug", 'wb') )
            if INFO:
                print "unexpected. need debug"
        # need to fix the bad state via enterTab()
        enterTab()
        return    

    maxPage = int(getMaxPage(soupB1))
    # set 100 items per page
    if (maxPage > 0):
        reqB1=resultPageSet100()
        soupB1=soup(reqB1)
        maxPage = int(getMaxPage(soupB1))
    
    currentPage = 0
    df = pd.DataFrame(columns=['kijNo','kijDtm','mgrCd', 'egMgrNm', 'hydiLnkOut', 'hydiSaiSnFlgOut', 'hydiLnkPsCd', 'kijSryoKbnCd6', 'psCd6', 'kkiKmkOut', 'kkiKmkSaiSnFlgOut', 'dwnladPsCd'])
    
    while (currentPage <= maxPage):
        resTag = getResultTag(soupB1)	
        if not resTag:
            print "something wrong at currPage={} maxPg={}: null result tag.".format(currentPage, maxPage)
            print soupB1
            csvFname = csvFname + ".cp{}.mp{}.null".format(currentPage, maxPage)
            break
        df = crunchResultToDF(resTag, df, alldocFlag, pathdir)

        currentPage += 1
        if (currentPage > maxPage):
            break
        reqB1 = resultGotoPage(currentPage)
        soupB1 = soup(reqB1)
    
    df.to_csv(csvFname, index=False, header=True, encoding='utf-8')
    return df
    #logout()

def run(date):
    ''' default single day job. for multiday, please run in iPython to avoid repeated login '''

    init() 
    enterJapan()
    enterTab()

    allDoc = True
    keywordQ= False
    # Keyword PDF/ZIP download
    runGeneral(date, keywordQ)

    logout()

if __name__ == "__main__":
    d = dd.today()
    yesterday = dd(d.year, d.month, d.day) - pd.datetools.timedelta(1)
    run(yesterday)
