#! /opt/python2.7/bin/python2.7

import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from optparse import OptionParser
import datetime as dt
import sql_io as sql_io
import time

url_base = 'https://www.hkex.com.hk/eng/stat/dmstat/OI/DTOP_O_%s.zip'

def run(start_date, end_date=None):

    if end_date is None:
        end_date = start_date

    bd_list = pd.DateRange(start_date, end_date, offset=pd.datetools.bday)
    for bd in bd_list:

        print bd
        d_str = bd.strftime('%Y%m%d')
        c1 = requests.get(url_base%d_str).content
        if '\xe4\xbd\x8d\xe7\xbd\xae\xe5\xb7\xb2\xe6\x9b\xb4\xe6\x94\xb9' in c1: break
        time.sleep(60)
        with open('./outputs/DTOP_O_%s.zip'%d_str, 'w') as f:
            f.write(c1)


if __name__ == '__main__':

    myparser = OptionParser()
    myparser.add_option('--start_date', dest='start_date', default=dt.datetime.today())
    myparser.add_option('--end_date', dest='end_date', default=None)
    myparser.add_option('--db', dest='db', action='store_true')
    opts, _ = myparser.parse_args()

    if opts.start_date is None:
        opts.start_date = dt.datetime.strptime(opts.start_date, '%Y-%m-%d')

    run(opts.start_date, opts.end_date)
