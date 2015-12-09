#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import random
import sys
import time

s = requests.Session()

import os
url_tmpl = "http://www.borsaistanbul.com/data/ehb/{:%Y}/{:%m}/ehb{:%Y%m%d}{}.zip"


def downloadBorsa(date):
    outdir = './outputs/borsa.bulletin/{:%Y}'.format(date)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    print date

    for sess in [1,2]:
        url= url_tmpl.format(date, date, date, sess)
        print url 

        test='''
        req2 = None
        try:
            req2 = s.request('GET', url)  #, allow_redirects=True)
        except:
            # if connection broken, wait and redo
            print "error, try again in 5 mins"
            pause(300)
            s = requests.Session()
            req2 = s.request('GET', url) 
    '''
        req2 = s.request('GET', url) 
        res = req2.headers.get('content-type')
        if pd.isnull(res):
            print "no data on {}".format(date)
            time.sleep(5) #5 sec
            break
        else:
            filepath = '{}/ehb{:%Y%m%d}{}.zip'.format(outdir, date, sess)
            with open(filepath, "wb") as f:
                f.write(req2.content)
            time.sleep(10) #20 sec

def downloadYear(year):
    for date in pd.DateRange(dd(year,1,2), dd(year,12,31), pd.datetools.BDay ):
        downloadBorsa(date)


if __name__ == '__main__':
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    downloadBorsa(day)

    #for date in pd.DateRange(dd(2004,2,19), dd(2004,12,31), pd.datetools.BDay ):
    #for date in pd.DateRange(dd(2015,8,4), dd(2015,9,22), pd.datetools.BDay ):
    #for date in pd.DateRange(dd(2015,9,23), dd(2015,10,19),pd.datetools.BDay ):
    #    downloadBorsa(date)
    #for y in xrange(1999,1997,-1):
    #    downloadYear(y)
    #for m in xrange(1,13):
    #	downloadBorsa(dd(1998,m,5))
    #downloadYear(2000)
    
