#!/opt/python2.7/bin/python2.7

import urllib2
from bs4 import BeautifulSoup
import requests
import urllib
import time
import datetime as dt
import re
import pandas as pd

url1 = "http://www.sebi.gov.in/sebiweb/investment/FIITrends.jsp"
url2 = "http://www.sebi.gov.in/sebiweb/investment/FIITrendsNewView.jsp"


def download(datadate, save=True):
    """
    download data from 1999/01/31 to 2014/05/31
    """
    if datadate <= dt.datetime(1999,1,1) or datadate > dt.datetime(2014,5,31):
        raise Exception('date is out of the range, do not use this function')

    if datadate <=dt.datetime(2009,11,30):
        url = url1
    else:
        url = url2

    hdnDate = datadate.strftime('%d/%m/%Y')
    hdnDate2 = datadate.strftime('%d-%b-%Y')
    print hdnDate
    data = {'print_url':'FIITrends_print.jsp', 'txtCalendar':hdnDate}

    r = requests.post(url, data).content
    s = BeautifulSoup(r)

    tbls = s.findAll('table')
    tbl = [i for i in tbls if ('Reporting Date' in i.text) and (str(i).count('table')==2)][0]

    if save:
        with open('/local/home/dwang/git_root/general/dwang/data_sourcing/fpi/nsdl/%s.xls'%hdnDate2, 'w') as f:
            f.write(str(tbl))
    else:
        return str(tbl)

def download_hist(sleept=10):
    """
    download historical data from 1999/01/31 to 2014/05/31
    """
    for i in pd.DateRange(dt.datetime(1999,1,1), dt.datetime(2014,6,1), offset=pd.datetools.MonthEnd()):
        download(i)
        time.sleep(sleept)
