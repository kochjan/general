#!/opt/python2.7/bin/python2.7

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import re
import time

TRY_MAX = 10
sse_flder = '/home/dwang/git_root/general/dwang/data_sourcing/lhb/sse_data/'
szse_flder = '/home/dwang/git_root/general/dwang/data_sourcing/lhb/szse_data/'

def eastmoney_lhb():
    url_base1 = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=2000,page=1,sortRule=-1,sortType=,startDate=%(startdate)s,endDate=%(enddate)s,gpfw=0,js=var%20data_tab_1.html?rt=24383405"
    url_base2 = "http://data.eastmoney.com/stock/lhb,%(date)s,%(ticker)s.html"

    date = dt.datetime(2016,5,11).strftime('%Y-%m-%d')

    url1 = url_base1%{'startdate':date, 'enddate':date}

    r = requests.get(url).content.decode('gb2312')

    data_l1 = eval(re.findall('"data":(\[.*\])', r)[0])

    df1 = pd.DataFrame(data_l1)

    df1 = df1[['SCode', 'SName', 'Ctypedes']].rename(columns={'SCode':'ticker', 'SName':'name', 'Ctypedes':'reason'}).applymap(lambda x: x.strip())

    for i in df1.iterrows():
        stock = i[1]
        url2 = url_base2%{'date':date, 'ticker':stock['ticker']}
        r2 = requests.get(url2).content.decode('gb2312')
        s2 = BeautifulSoup(r2)

################################################################################

def sse_lhb(date):
    count = 0
    datetx = date.strftime('%Y-%m-%d')
    while True:
        try:
            count += 1
            r2=requests.post('http://query.sse.com.cn/infodisplay/showTradePublicFile.do', data={"isPagination":False, 'dateTx':datetx}, headers={'Referer':'http://www.sse.com.cn/disclosure/diclosure/public/'})
            break
        except Exception,e:
            time.sleep(30)
            if count > TRY_MAX:
                return datetx
        
    c = r2.content
    #print c
    with open(sse_flder+datetx, 'w') as f:
        f.write(c)


def szse_lhb(date):
    count = 0
    datetx = date.strftime('%y%m%d')
    datetx2 = date.strftime('%Y-%m-%d')

    while True:
        try:
            count += 1
            r=requests.get('http://www.szse.cn/szseWeb/common/szse/files/text/jy/jy%s.txt'%datetx)
            break
        except Exception,e:
            time.sleep(30)
            if count > TRY_MAX:
                return datetx2
            
    c = r.content.decode('gb2312').encode('utf8')
    with open(szse_flder+datetx2, 'w') as f:
        f.write(c)


def sse_hist(start=dt.datetime(2006,8,7), end=dt.datetime.today()):
    fail_list = []
    for date in pd.DateRange(start, end):
        print date
        fail_list.append(sse_lhb(date))
        time.sleep(10)
    print [i for i in fail_list if i]

def szse_hist(start=dt.datetime(2003,9,1), end=dt.datetime.today()):
    fail_list = []
    for date in pd.DateRange(start, end):
        print date
        fail_list.append(szse_lhb(date))
        time.sleep(10)
    print [i for i in fail_list if i]
        
