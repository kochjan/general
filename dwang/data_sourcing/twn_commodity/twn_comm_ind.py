#!/opt/python2.7/bin/python2.7

import requests
import pandas as pd
import re
from bs4 import BeautifulSoup as bs
import time
import sql_io as sql_io
import datetime as dt

base_url = 'http://dmz9.moea.gov.tw/gmweb/investigate/InvestigateDB.aspx?lang=E'


c = requests.get(base_url).content
s = bs(c)
viewstate_tag = s.find('input', {'name':'__VIEWSTATE', 'id':'__VIEWSTATE'})
viewstate = re.findall('value="(.*)"', str(viewstate_tag))[0]

eventvalidation_tag = s.find('input', {'name':'__EVENTVALIDATION', 'id':'__EVENTVALIDATION'})
eventvalidation = re.findall('value="(.*)"', str(eventvalidation_tag))[0]

tds = s.findAll('td', {'nowrap':"nowrap"})
ckbx = [i for i in tds if 'checkbox' in str(i).lower()]
item2n_ckbx_not_all = [i for i in ckbx if ('tvItem2n' in str(i)) and ('all' not in str(i).lower())]
item2n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text.encode('utf-8') for i in item2n_ckbx_not_all}

def parse(tds):

    tmp_list = [i.text.replace(u'\xa0', '').replace(',', '') for i in tds[:-1]]
    return [int(tmp_list[0]) if tmp_list[0] else None, str(tmp_list[1])] + map(lambda x: float(x) if x.isdigit() else None, tmp_list[2:])


for item2n in item2n_dict:

    c = requests.get(base_url).content
    s = bs(c)
    viewstate_tag = s.find('input', {'name':'__VIEWSTATE', 'id':'__VIEWSTATE'})
    viewstate = re.findall('value="(.*)"', str(viewstate_tag))[0]

    eventvalidation_tag = s.find('input', {'name':'__EVENTVALIDATION', 'id':'__EVENTVALIDATION'})
    eventvalidation = re.findall('value="(.*)"', str(eventvalidation_tag))[0]
    
    c2 = requests.post(base_url, data={'__VIEWSTATE':viewstate, '__EVENTVALIDATION':eventvalidation, 'ctl00$ContentPlaceHolder1$hidLanguage':'E', 'ctl00$ContentPlaceHolder1$ddlPeriod':'M', 'ctl00$ContentPlaceHolder1$ddlDateBeg':199001, 'ctl00$ContentPlaceHolder1$ddlDateEnd':201508,\
'ctl00$ContentPlaceHolder1$ddlValueKind':'V', \
'ctl00$ContentPlaceHolder1$ddlQueryKind':'R', \
'ctl00$ContentPlaceHolder1$btnQuery':'Query', \
'ContentPlaceHolder1_tvItem1n0CheckBox':'on', \
'ContentPlaceHolder1_tvItem1n1CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n2CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n3CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n4CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n5CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n6CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n7CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n8CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n9CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n10CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n11CheckBox':'on',\
'ContentPlaceHolder1_tvItem1n12CheckBox':'on',\
item2n:'on'}).content

    s2 = bs(c2)
    trs_o = s2.findAll('tr')
    cols = [i for i in trs_o if 'HorVerCross_Empty' in str(i)][2:]
    col1 = [i.text.replace('(','').replace(')','').replace(',', '') for i in cols[0].findAll('td', {'class':'HorDim'})]
    #col2 = [i.text for i in cols[1].findAll('td', {'class':'HorDim'})]
    #col_l = [i[0]+': '+i[1] for i in zip(col1, col2)]
    trs = [i for i in trs_o if 'VerDim' in str(i)][2:]
    res_list = []
    for tr in trs:
        tds = tr.findAll('td')
        res_list.append(parse(tds))
    #import pdb; pdb.set_trace()
    res_df = pd.DataFrame(res_list, columns=['Year', 'Month']+col1)
    res_df['Industry'] = item2n_dict[item2n]
    res_df['Year'] = res_df['Year'].fillna()
    res_df['Year_Month'] = [dt.datetime.strptime(i, '%Y%b') for i in map(lambda x: str(int(x)), res_df['Year'])+res_df['Month']]
    res_df.columns = ['Year', 'Month', 'Indexes of Industrial Production 2011=100', "Indexes of Producer Shipments 2011=100",\
       "Indexes of Producer Inventory 2011=100",\
       "Indexes of Production Value 2011=100",\
       'Seasonally Adjusted Indexes 2011=100',\
       'Production Value NT$ 1000',\
                               'Sales Value NT$ 1000',\
                               'Domestic Sales Value Including Indirect Export NT$ 1000',\
                               'Direct Export Value NT$ 1000', \
                               'Inventory Value NT$ 1000', \
                               'Inventory Ratio %', 'Industry', 'Year_Month']
    
    sql_io.write_frame(res_df[['Industry', 'Year_Month', 'Indexes of Industrial Production 2011=100', "Indexes of Producer Shipments 2011=100",\
       "Indexes of Producer Inventory 2011=100",\
       "Indexes of Production Value 2011=100",\
       'Seasonally Adjusted Indexes 2011=100',\
       'Production Value NT$ 1000',\
                               'Sales Value NT$ 1000',\
                               'Domestic Sales Value Including Indirect Export NT$ 1000',\
                               'Direct Export Value NT$ 1000', \
                               'Inventory Value NT$ 1000', \
                               'Inventory Ratio %']], 'dwang..twn_comm_ind', if_exists='append')
    
    #import pdb; pdb.set_trace()
    time.sleep(10)
