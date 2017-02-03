#!/opt/python2.7/bin/python2.7
""" find the detailed description/exampls for each product"""

import requests
import pandas as pd
import re
from bs4 import BeautifulSoup as bs
import time
import sql_io as sql_io
import datetime as dt
from optparse import OptionParser
import nipun.dbc as dbc

base_url = 'http://dmz9.moea.gov.tw/gmweb/investigate/InvestigateDA.aspx?lang=E'


c = requests.get(base_url).content
s = bs(c)
viewstate_tag = s.find('input', {'name':'__VIEWSTATE', 'id':'__VIEWSTATE'})
viewstate = re.findall('value="(.*)"', str(viewstate_tag))[0]

eventvalidation_tag = s.find('input', {'name':'__EVENTVALIDATION', 'id':'__EVENTVALIDATION'})
eventvalidation = re.findall('value="(.*)"', str(eventvalidation_tag))[0]

tds = s.findAll('td', {'nowrap':"nowrap"})
ckbx = [i for i in tds if 'checkbox' in str(i).lower()]
item2n_ckbx_not_all = [i for i in ckbx if ('tvItem2n' in str(i)) and ('check all' not in str(i).lower())]
#item2n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text.encode('utf-8') for i in item2n_ckbx_not_all}
item2n_list = [re.findall('\((\d*)\)',i.text)[0] for i in item2n_ckbx_not_all]


def run(startm='201508', endm='201508'):

    for item2n in ['2399990', '2411122', '2421000','2422010','2433030','2433050','2521010','2522020','2544190','2934910','3219990','3220010','3314010','3400355','3400390','3400400']:#item2n_list[:50]:
        print item2n
        
        c2 = requests.post(base_url, data={'__VIEWSTATE':viewstate, '__EVENTVALIDATION':eventvalidation, \
                                           'ctl00$ContentPlaceHolder1$ScriptManager1':'ctl00$ContentPlaceHolder1$UpdatePanel7|ctl00$ContentPlaceHolder1$btnProduct', \
                                           'ctl00$ContentPlaceHolder1$hidLanguage':'E', 'ctl00$ContentPlaceHolder1$ddlPeriod':'M', 'ctl00$ContentPlaceHolder1$ddlDateBeg':startm, 'ctl00$ContentPlaceHolder1$ddlDateEnd':endm,\
    'ctl00$ContentPlaceHolder1$ddlValueKind':'V', \
    'ctl00$ContentPlaceHolder1$ddlQueryKind':'R', \
    'ctl00$ContentPlaceHolder1$hidId':item2n, \
    'ctl00$ContentPlaceHolder1$btnProduct':'Button', \
    'ContentPlaceHolder1_tvItem1_ExpandState':'nnnnnnnnnn' \
    }).content

        s2 = bs(c2)
        tbl = s2.findAll('table', {"class":"KindTable"})[0]
        trs = tbl.findAll('tr')[1:]
        product_exampls = [i.findAll('td')[0].text.strip() for i in trs]
        product_example_dict[item2n] = product_exampls
        print product_exampls
        #import pdb; pdb.set_trace()
        time.sleep(10)

if __name__=='__main__':

    product_example_dict={}
    run()
    res_df = pd.DataFrame([('(%s)'%i,j) for i in product_example_dict for j in product_example_dict[i] if j])
    res_df.columns=['Commodity', 'Examples']
    import pdb;pdb.set_trace()
    ewrtr = pd.ExcelWriter('./twn_prod_examples.xlsx')
    res_df.to_excel(ewrtr)
    ewrtr.save()

    
    f = pd.ExcelFile('/home/dwang/shared/khanh_twn_comm_clean.xlsx')
    khanh_df = f.parse(f.sheet_names[0])
    df = pd.merge(khanh_df, res_df, how='left', on=['Commodity'])
    ewrtr = pd.ExcelWriter('./khanh_twn_comm_final.xlsx')
    df.to_excel(ewrtr)
    ewrtr.save()
    
    
