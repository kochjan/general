#!/opt/python2.7/bin/python2.7

import pandas as pd
import requests
import nipun.dbc as dbc
from bs4 import BeautifulSoup
import re
import datetime as dt
import nipun.sql_io as sql_io
import time

dbo = dbc.db(connect='qai')
url1 = "http://www.idx.co.id/en-us/home/datadownload/summary.aspx"
TABLE = 'dwang..idn_foreign_flow'

def download_report(target_date):
    """
    download the report in xls file for the target date.
    """
    r = requests.get(url1)
    bs = BeautifulSoup(r.content)
    viewstate = bs.findAll('input', {"name":"__VIEWSTATE"})
    viewstate = re.findall('value="(.*)"', str(viewstate))[0]

    t_date = target_date.replace(hour=0, minute=0, second=0)#dt.datetime(2016,3,7)
    today = dt.datetime.today().replace(hour=0, minute=0, second=0)

    date1 = t_date.strftime('%Y-%m-%d')#'2016-03-08'
    date2 = t_date.strftime('%-m/%-d/%Y')#'3/8/2016'
    date3 = t_date.strftime('%Y-%m-%d-%M-%H-%S')#'2016-03-08-00-00-00'
    date4 = t_date.strftime('[[%Y,%-m,%-d]]')#'[[2016,3,8]]'
    date5 = today.strftime('%Y-%m-%d')#'2016-03-10'
    date6 = today.strftime('%-m/%-d/%Y')#'3/10/2016'
    date7 = today.strftime('%Y-%m-%d-%M-%H-%S')#'2016-03-10-00-00-00'


    payload1 = {'__EVENTTARGET':'dnn$ctr1000$MainView$lbSearch', '__VIEWSTATE':viewstate, 'dnn$ts$qbei':'Search', \
               "dnn$ctr1000$MainView$cbStockCode":"on", "dnn$ctr1000$MainView$cbVolume":"on", "dnn$ctr1000$MainView$cbFrequency":"on", \
               "dnn$ctr1000$MainView$cbForeignSell":"on", "dnn$ctr1000$MainView$cbForeignBuy":"on", "dnn$ctr1000$MainView$cbNonRegularVolume":"on", \
               "dnn$ctr1000$MainView$cbNonRegularValue":"on", "dnn$ctr1000$MainView$cbNonRegularFrequency":"on", "dnn$ctr1000$MainView$cbClose":"on", \
               "dnn$ctr1000$MainView$rgMain$ctl00$ctl03$ctl01$PageSizeComboBox":10, \
                'dnn$ctr1000$MainView$rdpDate':date1, \
               "dnn_ctr1000_MainView_rdpDate_dateInput_text":date2, \
                "dnn$ctr1000$MainView$rdpDate$dateInput": date3, \
                "dnn_ctr1000_MainView_rdpDate_calendar_SD": date4, \
                "dnn$ctr1000$MainView$rdpDateIS": date5, \
                "dnn_ctr1000_MainView_rdpDateIS_dateInput_text": date6, \
                "dnn$ctr1000$MainView$rdpDateIS$dateInput": date7, \
                "dnn$ctr1000$MainView$rdpBS": date5, \
                "dnn_ctr1000_MainView_rdpBS_dateInput_text": date6, \
                "dnn$ctr1000$MainView$rdpBS$dateInput": date7, \
                "dnn$ctr1000$MainView$rdpDateR": date5, \
                "dnn_ctr1000_MainView_rdpDateR_dateInput_text": date6, \
                "dnn$ctr1000$MainView$rdpDateR$dateInput": date7}

    r = requests.post(url1, data=payload1)

    payload2 = payload1
    payload2['__EVENTTARGET'] = 'dnn$ctr1000$MainView$lbDownlaod'
    r = requests.post(url1, data=payload2)

    fn = '/home/dwang/shared/idxdownload/idx_%s.xls'%date1
    with open(fn, 'w') as f:
        f.write(r.content)

    return fn

def parser(fn):
    """
    read and parse the faked xls file, which is actually html file
    """
    with open(fn, 'r') as f:
        tmp = f.read()

    ssindex = sorted(list(set(re.findall('ss:Index="(\d*)"', tmp))))
    bs = BeautifulSoup(tmp)

    df_list = []
    for ssi in ssindex:
        tmp = bs.findAll('cell', {'ss:index':ssi})
        df_list.append([i.text for i in tmp])

    df = pd.DataFrame(df_list)
    df.set_index(0, inplace=True)
    df = df.T
    cols = df.columns.tolist()
    cols.remove('Stock Code')
    for col in cols:
        df[col] = df[col].astype(float)
    df = df.reset_index(drop=True)
    
    datadate = re.findall('idx_(.*).xls', fn)[0]
    df['datadate'] = datadate

    df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    df = df.rename(columns={'Stock Code':'symbol', 'Close':'Close_price'})
    barrid_df = find_barrid(dt.datetime.strptime(datadate, '%Y-%m-%d'), listed_country='IDN')
    df = pd.merge(df, barrid_df, on=['symbol'], how='left')

    return df
        
def find_barrid(datadate, listed_country='IDN'):
    """
    find barrid to localid map
    """

    sql = """select barrid, localid, datadate from nipun_prod..security_master
    where listed_country='%(country)s' and '%(date)s' between datadate and isnull(stopdate, '2050-01-01')
    order by barrid, datadate"""

    df = dbo.query(sql%{'date':datadate.strftime('%Y-%m-%d'), 'country':listed_country}, df=True)
    df = df.drop_duplicates(cols=['barrid'], take_last=True)
    df.pop('datadate')
    df['symbol'] = [i[2:] for i in df['localid']]
    df.pop('localid')
    
    return df

def main(startdate=None, enddate=None, sleeptime=0):
    """

    """
    if startdate is None:
        startdate = dt.datetime.today()
        
    if enddate is None:
        enddate = dt.datetime.today()

    for date in pd.DateRange(startdate, enddate, offset=pd.datetools.day):
        print date.strftime('%Y-%m-%d')
        fn = download_report(date)
        df = parser(fn)
        sql_del = """delete from %(table)s where datadate='%(date)s'"""%{'table':TABLE, 'date':date.strftime('%Y-%m-%d')}
        dbo.cursor.execute(sql_del)
        dbo.commit()
        print "len(df) = %s"%len(df)
        print "start to upload to table %s"%TABLE
        sql_io.write_frame(df, TABLE, if_exists='append', bulk='off')
        time.sleep(sleeptime)


def backfill_hist(startdate=dt.datetime(2015,1,1), enddate=None):
    """
    backfill historical data
    """
    main(startdate, enddate, 60)


if __name__ == "__main__":
    main()
    
