#! /opt/python2.7/bin/python2.7

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from optparse import OptionParser
import datetime as dt
import sql_io as sql_io
import time
import os

url_base = 'https://www.hkex.com.hk/eng/stat/dmstat/OI/DTOP_O_%s.zip'
output_dir = './outputs/'

def run(start_date, end_date=None, forceall=False):

    if end_date is None:
        end_date = start_date

    bd_list = pd.DateRange(start_date, end_date, offset=pd.datetools.bday)
    d_str_l = [re.search('\d+', i).group() for i in os.listdir(output_dir)]
    
    for bd in bd_list:

        print bd
        d_str = bd.strftime('%Y%m%d')
        
        if (not forceall) and (d_str in d_str_l):
            continue
        
        c1 = requests.get(url_base%d_str).content
        if '\xe4\xbd\x8d\xe7\xbd\xae\xe5\xb7\xb2\xe6\x9b\xb4\xe6\x94\xb9' in c1: continue
        
        with open(output_dir+'/DTOP_O_%s.zip'%d_str, 'w') as f:
            f.write(c1)
            
        time.sleep(60)

if __name__ == '__main__':

    myparser = OptionParser()
    myparser.add_option('--start_date', dest='start_date', default=None)
    myparser.add_option('--end_date', dest='end_date', default=None)
    myparser.add_option('--forceall', dest='forceall', action='store_true')
    myparser.add_option('--db', dest='db', action='store_true')
    opts, _ = myparser.parse_args()

    if opts.start_date is not None:
        opts.start_date = dt.datetime.strptime(opts.start_date, '%Y-%m-%d')
    else:
        opts.start_date = dt.datetime.today()
        
    if opts.end_date is not None:
        opts.end_date = dt.datetime.strptime(opts.end_date, '%Y-%m-%d')
    else:
        opts.end_date = dt.datetime.today()

    run(opts.start_date, opts.end_date, opts.forceall)
