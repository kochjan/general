#!/opt/python2.7/bin/python2.7
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import sys
import time
from nipun import sql_io
from nipun.mailer import mail_me

def mail_status(stat):
    mail_me(['weijing.zhu@nipuncapital.com'], 'xueqiu1 '+stat,
        'check /local/home/dwang/git_root/general/dwang/data_sourcing/xueqiu/xueqiu1.log')

try:
    
    base_url = 'http://xueqiu.com/hq/screener/CN'
    url = """https://xueqiu.com/stock/screener/screen.json"""
    headers = {'user-agent':'Mozilla/5.0'}


    r=requests.get(base_url, headers=headers)


    #r2=requests.get(url='http://xueqiu.com/stock/screener/screen.json?\
    # category=SH&exchange=&areacode=&indcode=&orderby=follow7d&order=desc\
    # &current=ALL&pct=ALL&page=1&follow7d=0_5312&follow=33_707106&follow7dpct=0.02_64\
    # &tweet7dpct=0.05_107.73&tweet7d=0_1464&tweet=0_128090&deal=0_7366&deal7d=0_66&deal7dpct=0.05_38.89',
    # headers={'user-agent':'Mozilla/5.0'}, cookies=r.cookies)

    range = '0_{}'.format(sys.maxint)
    cols = ['symbol', 'name', 'follow', 'follow7d', 'follow7dpct', 'tweet',\
            'tweet7d', 'tweet7dpct', 'deal', 'deal7d', 'deal7dpct']

    # on the Xueqiu home page, select the 2nd stanza, 2nd vertical tab named 'Xueqiu indicators'
    # see 3 x 3 clickable selections
    # the 3 rows can be seen in column 1:  total-follow, weekly-new-follow, weekly-follow-growth-pct
    # the 3 columns are: follow, tweet(discussion), deal (transactions)
    # hence the 9 combinations of indicator
    # we seek full range for all these indicators.

    data = {'category':'SH', 'orderby':'follow7d', 'order':'desc', 'current':'ALL',\
            'pct':'ALL', 'page':0, 'follow7d':range, 'follow':range, 'follow7dpct':range,\
            'tweet7d':range, 'tweet':range, 'deal':range,'deal7d':range}
    # no longer useful as selector as of 20160907: 'tweet7dpct':range, 'deal7dpct':range,

    res_list = []
    while True:
        data['page'] += 1
        print data['page']

        r2=requests.get(url, headers=headers, cookies=r.cookies, params=data, timeout=30)
        res_dict = eval(r2.content.replace('null', 'None').replace('false', 'False'))
        res_list.extend(res_dict['list'])

        if data['page'] == 1:
            count = res_dict['count']
            n = int(pd.np.ceil(float(count) / len(res_dict['list'])))
            print 'total count = %s, in 1st page there are %s rows, totally %s pages.'%(count, len(res_dict['list']), n)

        if data['page'] == n:
            break

        time.sleep(1)

    res_df = pd.DataFrame(res_list)
    res_df = res_df[cols]
    res_df['symbol'] = [i.decode('utf8') for i in res_df['symbol'].apply(str.strip)]
    res_df['name'] = [i.decode('utf8') for i in res_df['name'].apply(str.strip)]
    res_df = res_df.rename(columns={'symbol':'localid'})
    res_df['datadate'] = dt.datetime.today().strftime('%Y-%m-%d')
    res_df = res_df.drop_duplicates()#there might be duplicated rows

    print res_df.head()
    sql_io.write_frame(res_df, 'dwang..xueqiu1', bulk='off', if_exists='append', unic=True)
    mail_status('success')
    
except Exception,e:
    mail_status('failure')
    print e
