#!/opt/python2.7/bin/python2.7

import xmlrpclib as xrl
import pandas as pd
import time
import os
from nipun.mailer import mail_me as mm
import nipun.dbc as dbc

dbo = dbc.db(connect='qai')
sql = '''
select top 1 * from production_task..task_queue
    where task_detail='%s'
     order by task_id desc
 '''

server = xrl.ServerProxy('http://localhost:8280/', use_datetime=True)
prev_failure = []

def output_file(df, fn='/home/dwang/git_root/general/dwang/daily_failures/daily_failures.csv'):
    if (df is None) or (len(df)==0):
        return
    if not os.path.exists(fn):
        df.to_csv(fn, index=None)
    else:
        with open(fn, 'a') as f:
            for i in df.values:
                f.write(','.join([str(j) for j in i])+'\n')

while True:
    try:
        res = server.task_report()

        head = res[0]
        data = res[1:]
        x=pd.DataFrame(data, columns=head)
        failed_df = x[x['status']=='FAILED']

        index_list = []
        for i in failed_df.iterrows():
            if i[1]['detail'] not in prev_failure:
                index_list.append(i[0])

        if index_list:
            output_file(failed_df.ix[index_list])
            s = failed_df.ix[index_list].to_html()+'<br/>'
            for i in failed_df.ix[index_list]['detail']:
                s += str(i)+'<br/>'+dbo.query(sql%i, df=True)['comments'].values[0]+'<br/>'+'='*40+'<br/>'
            mm(['ding.wang@nipuncapital.com'], 'daily task newly failed', s, msg_type='html')

        prev_failure = list(failed_df['detail'])

        #time.sleep(60*5)
    except Exception, e:
        server = xrl.ServerProxy('http://localhost:8280/', use_datetime=True)
    finally:
        time.sleep(60*2)
