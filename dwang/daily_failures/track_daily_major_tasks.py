#!/opt/python2.7/bin/python2.7

import xmlrpclib as xrl
import pandas as pd
import time
import os
from nipun.mailer import mail_me as mm
import nipun.dbc as dbc
import sys

server = xrl.ServerProxy('http://localhost:8289/', use_datetime=True)

task_list = ['morning_avail', 'npxchnpak_alphav5', 'npxchnpak_d0_price', 'daily_ms_settle', \
             'daily_prelim_rpt', 'daily_holdings_prlm', 'pb_ipts_ovrd_daily', \
             'npx_rate_targeter', 'mpb_asia']

for tk in task_list:

    prev_tk_st = None
    sys.stdout.write('\n\n%s: '%tk)
    while True:
        res = server.task_report()
        head = res[0]
        data = res[1:]
        x=pd.DataFrame(data, columns=head)
        tk_st = x[x['detail']==tk]['status'].values[0]
        
        if tk_st is None:
            raise Exception('error in getting status')
        #print prev_tk_st, tk_st
        if prev_tk_st==tk_st:
            sys.stdout.write('.')
            sys.stdout.flush()
        else:
            sys.stdout.write('\n%s'%tk_st)

        if tk_st=='FAILED':
            raise Exception('Critical task failed, please check!!!')
        if tk_st=='COMPLETE':
            break
        prev_tk_st=tk_st
        time.sleep(10)

