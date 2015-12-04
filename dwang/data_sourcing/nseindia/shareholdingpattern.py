#!/opt/python2.7/bin/python2.7

"""

"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import nipun.dbc as dbc
import re
import time
import sql_io
import os
from optparse import OptionParser

base_url = "http://www.nseindia.com/corporates/shldStructure/"
jsp_high = "getShareholdingPatterns.jsp"
jsp_low_old = "shareholding_main.jsp"
jsp_low_new = "shareholding_main_aft_31Mar09.jsp"

## url_all_tickers = "http://www.nseindia.com/corporates/shldStructure/getShareholdingPatterns.jsp?start=0&limit=2000"
## url_all_dates = "http://www.nseindia.com/corporates/shldStructure/getShareholdingPatterns.jsp?start=0&limit=2000&symbol=BSL"
## url_old = "http://www.nseindia.com/corporates/shldStructure/ShareholdingPattern/shareholding_main.jsp?ndsId=10503&symbol=TATACOMM&countStr=4|1|0|1|1|0|0|0|3|0|0|0|0|0|New|0&asOnDate=30-Sep-2008&industry=-"
## url_new = "http://www.nseindia.com/corporates/shldStructure/ShareholdingPattern/shareholding_main_aft_31Mar09.jsp?ndsId=104147&symbol=DSSL&countStr=10%7C3%7C1%7C0%7C0%7C3%7C0%7C0%7C7%7C27%7C0%7C0%7C0%7C0%7CNew%7C1&asOnDate=30-Sep-2015&industry=-#"

success = 'success'
true = 'true'
results = 'results'
rows = 'rows'
recordID = "recordID"
symbol = "symbol"
CompanyName = "CompanyName"
ISIN = "ISIN"
Ind = "Ind"
asOnDate = "asOnDate"
countString = "countString"
Pattern = "Pattern"


def all_comp_names(save2f=True, rff=True):#rff read file first

    fn = './data/all_comp_names.csv'
    dodld = False # do download
    
    if rff:
        if os.path.exists(fn):
            all_df = pd.read_csv(fn)
        else:
            dodld = True
            
    if (not rff) or dodld:
        all_list = requests.post(base_url+jsp_high, data={'start':0, 'limit':2000}, timeout=30).content.strip()
        records = eval(all_list)
        if records[success] != 'true': raise Exception('all_list failed.')
        all_df = pd.DataFrame(records[rows])

        if save2f:
            all_df.to_csv(fn, index=None)
        
    return all_df


def all_dates(symb, save2f=True, rff=True):#for one company, find all qtr ends

    errf = open('./data/errors.err', 'a')
    fn = './data/'+symb+'_dates.csv'
    dodld = False # do download
    
    try:

        if rff:
            if os.path.exists(fn):
                all_list_dates_df = pd.read_csv(fn)
            else:
                dodld = True

        if (not rff) or dodld:
            all_list_dates_df = pd.DataFrame()
            all_dates = requests.post(base_url+jsp_high, data={'start':0, 'limit':2000, 'symbol':symb}, timeout=30).content.strip()
            all_dates_records = eval(all_dates)
            all_dates_df = pd.DataFrame(all_dates_records[rows])
            all_list_dates_df = all_list_dates_df.append(all_dates_df, ignore_index=True)

            if save2f:
                all_list_dates_df.to_csv(fn, index=None)
            print "all dates for %s"%symb
        return all_list_dates_df

    except Exception, e:
        errf.write(fn+'\n')
    finally:
        errf.close()


def get_all_on_a_date(dt_str, save2f=True):#dt_str like '30-SEP-2015'
    all_on_this_date = requests.post(base_url+jsp_high, data={'start':0, 'limit':2000, 'asOnDate':dt_str}, timeout=30).content.strip()
    all_on_this_date_records = eval(all_on_this_date)
    all_on_this_date_df = pd.DataFrame(all_on_this_date_records[rows])

    if save2f:
        all_on_this_date_df.to_csv('./data/'+'all_comp_on_%s.csv'%dt_str, index=None)
    return all_on_this_date_df


def parse_new(r):

    cat_shhder1_dict = {'A': 'Shareholding of Promoter and Promoter Group', 'B':'Public shareholding', 'C':'Shares held by Custodians and against which Depository Receipts have been issued'}
    cat_shhder2_dict = {('A',1): 'Indian', ('A',2): 'Foreign', ('B', 1): 'Institutions', ('B', 2):'Non-institutions'}
    s = BeautifulSoup(r)
    tables = s.findAll('table')
    trs = tables[-1].findAll('tr')
    cat_shhder1 = None
    cat_shhder2 = None
    res_list=[]
    s2f = lambda x: float(x) if x.replace('.','').isdigit() else None
    for tr in trs[2:]:
        if 'total' in tr.text.lower(): continue
        
        col0 = (cat_shhder1_dict.get(cat_shhder1, '')+' '+cat_shhder2_dict.get((cat_shhder1, cat_shhder2), '')).strip()
        tds = tr.findAll('td')
        if len(tds)==9:
            col1 = tds[1].text.strip().replace(',', '')
            col2 = s2f(tds[2].text.strip().replace(',', ''))
            col3 = s2f(tds[3].text.strip().replace(',', ''))
            col4 = s2f(tds[4].text.strip().replace(',', ''))
            col5 = s2f(tds[5].text.strip().replace(',', ''))
            col6 = s2f(tds[6].text.strip().replace(',', ''))
            col7 = s2f(tds[7].text.strip().replace(',', ''))
            col8 = s2f(tds[8].text.strip().replace(',', ''))
        elif len(tds)==2:
            tmp = tds[0].text.strip()
            if tmp == u'(A)':
                cat_shhder1 = 'A'
            elif tmp == u'(B)':
                cat_shhder1 = 'B'
            elif tmp == u'(C)':
                cat_shhder1 = 'C'
            elif tmp == u'(1)':
                cat_shhder2 = 1
            elif tmp == u'(2)':
                cat_shhder2 = 2
            continue
        else:
            continue

        col9 = [i for i in s.findAll('tr') if 'As on' in i.text][-1].findAll('td')[-1].text
        col10 = [i for i in s.findAll('tr') if 'nse symbol' in i.text.lower()][-1].findAll('td')[-1].text
        res_list.append([col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10])

    return res_list


def parse_old(r):

    cat_shhder1_dict = {'A': 'Shareholding of Promoter and Promoter Group', 'B':'Public shareholding', 'C':'Shares held by Custodians and against which Depository Receipts have been issued'}
    cat_shhder2_dict = {('A',1): 'Indian', ('A',2): 'Foreign', ('B', 1): 'Institutions', ('B', 2):'Non-institutions'}
    s = BeautifulSoup(r)
    tables = s.findAll('table')
    trs = tables[-1].findAll('tr')
    cat_shhder1 = None
    cat_shhder2 = None
    res_list=[]
    s2f = lambda x: float(x) if x.replace('.','').isdigit() else None
    for tr in trs[1:]:
        if 'total' in tr.text.lower(): continue
        
        col0 = (cat_shhder1_dict.get(cat_shhder1, '')+' '+cat_shhder2_dict.get((cat_shhder1, cat_shhder2), '')).strip()
        tds = tr.findAll('td')
        if len(tds)==7:
            col1 = tds[1].text.strip().replace(',', '')
            col2 = s2f(tds[2].text.strip().replace(',', ''))
            col3 = s2f(tds[3].text.strip().replace(',', ''))
            col4 = s2f(tds[4].text.strip().replace(',', ''))
            col5 = s2f(tds[5].text.strip().replace(',', ''))
            col6 = s2f(tds[6].text.strip().replace(',', ''))
            col7 = None
            col8 = None
        elif len(tds)==2:
            tmp = tds[0].text.strip()
            if tmp == u'(A)':
                cat_shhder1 = 'A'
            elif tmp == u'(B)':
                cat_shhder1 = 'B'
            elif tmp == u'(C)':
                cat_shhder1 = 'C'
            elif tmp == u'(1)':
                cat_shhder2 = 1
            elif tmp == u'(2)':
                cat_shhder2 = 2
            continue
        else:
            continue

        if not col1: continue
        col9 = [i for i in s.findAll('tr') if 'Quarter Ended on'.lower() in i.text.lower()][-1].findAll('td')[-1].text
        col10 = [i for i in s.findAll('tr') if 'nse symbol' in i.text.lower()][-1].findAll('td')[-1].text
        res_list.append([col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10])

    return res_list



def downloader(all_list_dates_df, col_n, w2file = True, db = True, fromfilefirst=False, noparse=False):

    with open('./data/errors.err', 'a') as errf:
        for i in all_list_dates_df.iterrows():
            try:
                tmp_S = i[1]
                fn = './data/'+tmp_S['symbol']+'_'+tmp_S['asOnDate']+'.txt'
                check_web = True
                ###########################
                if fromfilefirst:#read from ./data/files
                    if os.path.exists(fn):
                        with open(fn, 'r') as f:
                            r = f.read()
                            check_web = False

                ###########################
                if (not fromfilefirst) or (check_web):# read from website
                    jsp_str = jsp_low_new if tmp_S['Pattern']=='New' else jsp_low_old
                    r = requests.post(base_url+'ShareholdingPattern/'+jsp_str, data={'ndsId':tmp_S['recordID'], 'symbol':tmp_S['symbol'], 'countStr':tmp_S['countString'], 'asOnDate':tmp_S['asOnDate'],'industry':tmp_S['Ind']}, timeout=30).content
                    print fn
                    print 'wait for 30s'
                    time.sleep(30)

                    if w2file:
                        with open(fn, 'w') as f:
                            f.write(r)
                ############################
                if not noparse:
                    if 'nse symbol' not in r.lower():
                        raise Exception('nse symbol is not here')
                    parse = parse_new if tmp_S['Pattern']=='New' else parse_old
                    df = pd.DataFrame(parse(r), columns = col_n)
                    df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                    if db:
                        sql_io.write_frame(df, 'dwang..india_share', bulk='off', if_exists='append', unic=True)

            except Exception,e:
                errf.write(tmp_S['symbol']+'_'+tmp_S['asOnDate']+'\n')
                #errf.write('\n---------------------------------------------------\n')
        
if __name__ == '__main__':

    myparser = OptionParser()
    myparser.add_option('--fromfilefirst', '-f', dest='fff', action='store_true')
    myparser.add_option('--qtrend', '-q', dest='qtrend', default=None)#should be like '30-SEP-2015'
    myparser.add_option('--noparse', dest='noparse', action='store_true')
    opts, _ = myparser.parse_args()
    col_n = ['category', 'category of shareholder', 'number of shareholders', 'total number of shares', 'number of shares held in dematerialized form', 'pct of shares (A+B)', 'pct of shares (A+B+C)', 'number of shares', 'pct No. of shares', 'asondate', 'nse symbol']

    if opts.qtrend:#if give a specific quarter end date
        all_list_on_a_date_df = get_all_on_a_date(dt_str)
        downloader(all_list_on_a_date_df, col_n, fromfilefirst=opts.fff, noparse=opts.noparse)
    
    else:

        all_df = all_comp_names(rff=opts.fff) # names for all companies
        for symb in all_df['symbol']:
            print "start %s --------------"%symb
            all_list_dates_df = all_dates(symb, rff=opts.fff) # all (name, date) combination
            if all_list_dates_df:
                print 'wait for 20s'
                time.sleep(20)
                downloader(all_list_dates_df, col_n, fromfilefirst=opts.fff, noparse=opts.noparse)
            print "end %s --------------"%symb
    
