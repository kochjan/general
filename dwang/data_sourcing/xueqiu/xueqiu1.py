#!/opt/python2.7/bin/python2.7
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
import sys
import time
import sql_io
from nipun.mailer import mail_me


try:
    
    base_url = 'http://xueqiu.com/hq/screener/CN'
    url = """http://xueqiu.com/stock/screener/screen.json"""
    headers = {'user-agent':'Mozilla/5.0'}


    r=requests.get(base_url, headers=headers)


    #r2=requests.get(url='http://xueqiu.com/stock/screener/screen.json?category=SH&exchange=&areacode=&indcode=&orderby=follow7d&order=desc&current=ALL&pct=ALL&page=1&follow7d=0_5312&follow=33_707106&follow7dpct=0.02_64&tweet7dpct=0.05_107.73&tweet7d=0_1464&tweet=0_128090&deal=0_7366&deal7d=0_66&deal7dpct=0.05_38.89', headers={'user-agent':'Mozilla/5.0'}, cookies=r.cookies)

    range = '0_%(maxint)s'%{'maxint':sys.maxint}
    cols = ['symbol', 'name', 'follow', 'follow7d', 'follow7dpct', 'tweet', 'tweet7d', 'tweet7dpct', 'deal', 'deal7d', 'deal7dpct']

    data = {'category':'SH', 'orderby':'follow7d', 'order':'desc', 'current':'ALL', 'pct':'ALL', 'page':0, 'follow7d':range, 'follow':range, 'follow7dpct':range, 'tweet7dpct':range, 'tweet7d':range, 'tweet':range, 'deal':range,'deal7d':range, 'deal7dpct':range}

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

        time.sleep(0)

    res_df = pd.DataFrame(res_list)
    res_df = res_df[cols]
    res_df['symbol'] = [i.decode('utf8') for i in res_df['symbol'].apply(str.strip)]
    res_df['name'] = [i.decode('utf8') for i in res_df['name'].apply(str.strip)]
    res_df = res_df.rename(columns={'symbol':'localid'})
    res_df['datadate'] = dt.datetime.today().strftime('%Y-%m-%d')
    res_df = res_df.drop_duplicates()#there might be duplicated rows

    print sql_io
    sql_io.write_frame(res_df, 'dwang..xueqiu1', bulk='off', if_exists='append', unic=True)
    mail_me(['ding.wang@nipuncapital.com'], 'xueqiu1 was successful', 'check /local/home/dwang/git_root/general/dwang/data_sourcing/xueqiu/xueqiu1.log')
    
except Exception,e:
    mail_me(['ding.wang@nipuncapital.com'], 'xueqiu1 error', 'check /local/home/dwang/git_root/general/dwang/data_sourcing/xueqiu/xueqiu1.log')
    print e
