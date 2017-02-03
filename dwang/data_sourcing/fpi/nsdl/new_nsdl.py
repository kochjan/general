#!/opt/python2.7/bin/python2.7

import urllib2
from bs4 import BeautifulSoup
import requests
import urllib
import time
import datetime as dt
import re
import pandas as pd

url = "https://www.fpi.nsdl.co.in/web/Reports/Archive.aspx"

def downloader_new(datadate, save=True):
    """
    download the 2 tables
    """
    if datadate <= dt.datetime(2014,5,31):
        raise Exception('need to use the old website, not this function')

    hdnDate = datadate.strftime('%d-%b-%Y')
    hdnDate2 = datadate.strftime('%Y-%b-%d')
    print hdnDate
    r = urllib2.Request(url)
    resp = urllib2.urlopen(r)
    r2 = resp.read()
    s = BeautifulSoup(r2)
    VIEWSTATE = re.findall('value=\"(.*)\"\/', str(s.findAll('input', {'id':'__VIEWSTATE'})[0]))[0]
    EVENTVALIDATION = re.findall('value=\"(.*)\"\/', str(s.findAll('input', {'id':'__EVENTVALIDATION'})[0]))[0]
    data = urllib.urlencode({'__EVENTTARGET':'btnSubmit1', '__VIEWSTATE':VIEWSTATE, '__EVENTVALIDATION':EVENTVALIDATION, 'hdnDate':hdnDate})
    r3 = urllib2.urlopen(url, data)
    r4 = r3.read()

    s = BeautifulSoup(r4)
    t=s.findAll('table')
    month_str = ''.join([str(t[1]), str(t[-1])])

    if save:
        with open('/local/home/dwang/git_root/general/dwang/data_sourcing/fpi/nsdl/%s.xls'%hdnDate2, 'w') as f:
            f.write(month_str)
    else:
        return month_str


def download_hist(sleept=10):
    """
    download historical data from 2014-06-01
    """
    for i in pd.DateRange(dt.datetime(2014,6,1), dt.datetime.today(), offset=pd.datetools.MonthEnd()):
        downloader_new(i)
        time.sleep(sleept)

if __name__ == '__main__':
    
    downloader_new(dt.datetime.today())
