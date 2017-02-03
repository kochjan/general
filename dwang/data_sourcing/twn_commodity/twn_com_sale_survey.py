#!/opt/python2.7/bin/python2.7
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup as bs
import time
import sql_io as sql_io
import datetime as dt

base_url = 'http://dmz9.moea.gov.tw/gmweb/investigate/InvestigateEA.aspx?lang=E'
db_name = 'dwang..twn_com_sale_survey'
beg_month = 199001

c = requests.get(base_url).content
s = bs(c)
viewstate_tag = s.find('input', {'name':'__VIEWSTATE', 'id':'__VIEWSTATE'})
viewstate = re.findall('value="(.*)"', str(viewstate_tag))[0]

eventvalidation_tag = s.find('input', {'name':'__EVENTVALIDATION', 'id':'__EVENTVALIDATION'})
eventvalidation = re.findall('value="(.*)"', str(eventvalidation_tag))[0]

tds = s.findAll('td', {'nowrap':"nowrap"})
ckbx = [i for i in tds if 'checkbox' in str(i).lower()]
item1n_ckbx_not_all = [i for i in ckbx if ('tvItem1n' in str(i)) and ('check all' not in str(i).lower())]
#item1n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text.encode('utf-8') for i in item1n_ckbx_not_all}
item1n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text for i in item1n_ckbx_not_all}
item2n_ckbx_not_all = [i for i in ckbx if ('tvItem2n' in str(i)) and ('check all' not in str(i).lower())]
#item2n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text.encode('utf-8') for i in item2n_ckbx_not_all}
item2n_dict = {re.findall('id="(.*?)"', str(i))[0]:i.text for i in item2n_ckbx_not_all}

selectbeg = s.findAll('select', {'id':'ContentPlaceHolder1_ddlDateBeg'})[0]
beg_str = re.search('value="(\d+)"', str(selectbeg.findAll('option')[-1])).groups()[0]
selectend = s.findAll('select', {'id':'ContentPlaceHolder1_ddlDateEnd'})[0]
end_str = re.search('value="(\d+)"', str(selectend.findAll('option')[0])).groups()[0]

def parse(tds):

    tmp_list = [i.text.replace(u'\xa0', '').replace(',', '') for i in tds[:-1]]
    return [int(tmp_list[0]) if tmp_list[0] else None, str(tmp_list[1])] + map(lambda x: float(x) if (x[0].replace('.','').isdigit() or x[-1].replace('.','').isdigit()) else None, tmp_list[2:])


for item2n in item2n_dict:

    c = requests.get(base_url).content
    s = bs(c)
    viewstate_tag = s.find('input', {'name':'__VIEWSTATE', 'id':'__VIEWSTATE'})
    viewstate = re.findall('value="(.*)"', str(viewstate_tag))[0]

    eventvalidation_tag = s.find('input', {'name':'__EVENTVALIDATION', 'id':'__EVENTVALIDATION'})
    eventvalidation = re.findall('value="(.*)"', str(eventvalidation_tag))[0]

    data = {'__VIEWSTATE':viewstate, '__EVENTVALIDATION':eventvalidation, 'ctl00$ContentPlaceHolder1$hidLanguage':'E', 'ctl00$ContentPlaceHolder1$ddlPeriod':'M', 'ctl00$ContentPlaceHolder1$ddlDateBeg':beg_month if beg_month > int(beg_str) else beg_str, 'ctl00$ContentPlaceHolder1$ddlDateEnd':end_str,\
'ctl00$ContentPlaceHolder1$ddlValueKind':'V', \
'ctl00$ContentPlaceHolder1$ddlQueryKind':'R', \
'ctl00$ContentPlaceHolder1$btnQuery':'Query', \
item2n:'on'}
    data.update({i:'on' for i in item1n_dict})
    
    c2 = requests.post(base_url, data=data).content

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
    
    res_df = res_df.drop(['Year', 'Month'], axis=1)
    
    sql_io.write_frame(res_df, db_name, if_exists='append', unic=True)
    
    #import pdb; pdb.set_trace()
    time.sleep(10)
