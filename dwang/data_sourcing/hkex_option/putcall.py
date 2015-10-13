#! /opt/python2.7/bin/python2.7

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from optparse import OptionParser
import datetime as dt
import sql_io as sql_io
import time

url_base = 'https://www.hkex.com.hk/eng/sorc/market_data/statistics_putcall_ratio.aspx'
c1 = requests.get(url_base).content
s = BeautifulSoup(c1)
options = s.findAll('select', {'id':"underlying"})[0].findAll('option')
ucodes = [re.search('value="(.*)"', str(i)).groups()[0] for i in options]
res_list = []
time.sleep(10)

def run(start_date, end_date, ucodes_l=None):

    if ucodes_l is None:
        ucodes_l = ucodes
        
    for ucode in ucodes_l:
        print ucode
        c_tot = requests.post(url_base, data={'action':'ajax', 'type':'getTotal', 'ucode':ucode,'date_form':start_date,'date_to':end_date,'page':1}).content
        n_rows = int(re.search('-->(\d+)', c_tot).groups()[0])
        acc_row = 0
        page = 0

        while acc_row < n_rows:
            page += 1
            c2 = requests.post(url_base, data={'action':'ajax', 'type':'search', 'ucode':ucode,'date_form':start_date,'date_to':end_date,'page':page}).content
            s2 = BeautifulSoup(c2)
            row_list = s2.findAll('tr')
            for r in row_list:
                res_list.append([ucode]+[i.text.replace(',','') for i in r.findAll('td')])

            acc_row += len(row_list)
            #import pdb; pdb.set_trace()
        time.sleep(10)

if __name__ == '__main__':

    myparser = OptionParser()
    myparser.add_option('--ucodes', dest='ucodes', default=None)
    myparser.add_option('--start_date', dest='start_date', default=None)
    myparser.add_option('--end_date', dest='end_date', default=None)
    myparser.add_option('--db', dest='db', action='store_true')
    opts, _ = myparser.parse_args()

    d = dt.datetime.today() - dt.timedelta(1)
    d_str = dt.datetime.strftime(d, '%Y/%m/%d')
    
    if opts.start_date is None: opts.start_date = d_str
    if opts.end_date is None: opts.end_date = d_str
    if opts.ucodes is not None: opts.ucodes = opts.ucodes.split(',')

    run(opts.start_date, opts.end_date, opts.ucodes)
    res_df = pd.DataFrame(res_list, columns=['ticker', 'date', 'call', 'put', 'put call ratio'])
    res_df['date'] = [dt.datetime.strptime(i, '%d/%m/%Y') for i in res_df['date']]
    res_df['call'] = res_df['call'].apply(float)
    res_df['put'] = res_df['put'].apply(float)
    res_df['put call ratio'] = res_df['put call ratio'].apply(float)

    if opts.db:
        
        sql_io.write_frame(res_df, 'dwang..hk_putcall', if_exists='append', bulk='off')
    else:
        print res_df
        
