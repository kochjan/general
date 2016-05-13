#!/opt/python2.7/bin/python2.7

import requests
from bs4 import BeautifulSoup
import datetime as dt
import time
import pandas as pd

url = "http://www.taifex.com.tw/eng/eng7/eng7_10.asp"

year = 1998
month = 7
def download(year, month):
    r = requests.post(url, data={'syear':year, 'smonth':month}).content
    s = BeautifulSoup(r)
    ts = s.findAll('table')
    for i in ts:
        if 'Equity Index Futures' in i.text:
            table = i
            break

    if 'table' in locals(): 
        with open('./%s_%s.xls'%(year, month), 'w') as f:
            f.write(str(table))
    else:
        raise(Exception("table doesn't exists"))

if __name__ == '__main__':

    for i in pd.DateRange(dt.datetime(1998,7,1), dt.datetime.today(), offset=pd.datetools.MonthEnd()):
        print i
        download(i.year, i.month)
        time.sleep(20)
