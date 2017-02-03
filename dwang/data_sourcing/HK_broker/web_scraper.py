from __future__ import print_function
import requests
import re
from bs4 import BeautifulSoup
import datetime as dt
import nipun.dbc as dbc
import sql_io as mysqlio
import pandas as pd
import numpy as np
from optparse import OptionParser
import sys
from nipun.mailer import mail_me
import time


sql_str = """select barrid, localid, datadate as startdate, isnull(stopdate, '2050-01-01') as stopdate from nipun_prod..security_master where listed_country = 'HKG'"""
dbo = dbc.db(connect='qai')
bar_localid_df = dbo.query(sql_str, df=True)

def parse():

    myparser = OptionParser()
    myparser.add_option('--stdout', dest='stdout', action='store_true')
    myparser.add_option('--db', dest='db', action='store_true')
    myparser.add_option('--forceall', dest='forceall', action='store_true')
    myparser.add_option('--email', dest='email', action='store_true')
    return myparser.parse_args()

            
def match_barrid(res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left'):#from localid to barrid
    
    tmp_df = pd.merge(res_df, bar_localid_df, left_on=left_on, right_on=right_on, how=how)
    if isinstance(tmp_df['datadate'][0], str):
        tmp_df['datadate'] = tmp_df['datadate'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'))
    tmp_df = tmp_df[(tmp_df['datadate']>=tmp_df['startdate']) & (tmp_df['datadate']<=tmp_df['stopdate'])]
    tmp_df = tmp_df.sort(columns=['startdate'])
    tmp_df = tmp_df.drop_duplicates(cols=['datadate', 'ticker', 'category', 'value', 'source', 'sm_localid', 'localid'], take_last=True)

    if len(tmp_df) > len(res_df):
        raise Exception('Multiple barrid matched.')

    return tmp_df
    

class ExpRetry(Exception):
    pass

class ExpStopThisOne(Exception):
    pass

class ExpStopAll(Exception):
    pass


class WebScraper(object):
    
    def __init__(self, stdout=True, t_wait_time=600, t_inc=300, outputdir='/home/dwang/work/pub_log/HK_broker/'):
        self.list = self.__gen_list__()
        self.t_wait_time = t_wait_time
        self.t_inc = t_inc
        self.res_df = pd.DataFrame()
        self.stdout = stdout
        self.outputdir = outputdir
        self.url_wrong_l=[]
        if stdout:
            self.f = sys.stdout
        else:
            self.f = open(outputdir+self.__class__.__name__+'.log', 'w')

    def __gen_list__(self, **kv):

        raise Exception('you need to write a gen_list function')

    def scrape_sig_page(self, url):

        return requests.get(url).content

    def parse_page(self):

        raise Exception('you need to write a function to parse a page')

    def upload_to_DB(self):#this is only for HK local analyst project
        
        pk_e = False
        if len(self.res_df)>0:
            self.res_df = self.res_df.dropna(subset=['datadate', 'barrid', 'category', 'value', 'source'])
            pk_e = mysqlio.write_frame(self.res_df, 'dwang..hk_analyst', bulk='off', if_exists='append', unic=True)
        return pk_e

    def save_results(self):

        raise Exception('you need to write a function to save result to files')

    def run(self, i):#do web scraping for one page or one id, and then parse it.
                     #i must contain url and other post data
        T = 0
        while True:
            try:
                self.page_str = self.scrape_sig_page(i)
                self.parsed_page = self.parse_page()
                break
            except ExpRetry, e:
                self.prt(e)
                self.prt('will retry')
                if T <= self.t_wait_time:
                    time.sleep(T+np.random.random())
                    T+=self.t_inc
                else:
                    self.prt('Give up %s'%str(i))
                    break
            except ExpStopThisOne, e:
                self.prt(e)
                self.prt('stop this one %s'%str(i))
                break
            except ExpStopAll, e:
                self.prt('stop all')
                self.prt(e)
                raise(e)

    def __get_new__(self):

        raise Exception('you need to write a function to check if new records are there')

    def run_all(self, DB=False, force_all=False):
        
        for url in self.list:
            self.prt('-'*77)
            self.prt(url)
            try:
                self.run(url)#self.res_df should be populated
            #if needed, do aggregation
            
                if DB:
                    pk_e = self.upload_to_DB()#or, upload_to_DB
                    if (not force_all) and pk_e:#primary key vialation was met, it means the data are old, we don't need to scrape more pages
                        self.prt("New data have been updated. Stop now.")
                        break
            
            except Exception,e:
                self.prt(e)
                self.url_wrong_l.append(url)

        self.prt('error urls:')
        self.prt(self.url_wrong_l)
                
        #if needed, upload aggregated results to DB

    def prt(self, s):

        #print(str(s).encode('utf-8'), file=self.f)
        if isinstance(s, unicode):
                s = s.encode('utf-8')
                
        if self.f==sys.stdout:
            print(s)
        else:
            self.f.writelines(str(s))
            self.f.write('\n')
            self.f.flush()

    def __del__(self):

        self.f.close()

    def email(self):

        if not self.f.closed:
            self.f.close()

        if not self.stdout:
            with open(self.outputdir+self.__class__.__name__+'.log', 'r') as f:
                log = f.read()
            mail_me(['weijing.zhu@nipuncapital.com'], self.__class__.__name__, log)
        else:
            mail_me(['weijing.zhu@nipuncapital.com'], self.__class__.__name__, 'error urls:\n'+str(self.url_wrong_l))#log)

    
