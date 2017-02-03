#!/opt/python2.7/bin/python2.7

import xmlrpclib as xrl
import pandas as pd
import time
import os
from nipun.mailer import mail_me as mm
import nipun.dbc as dbc
import sys

server = xrl.ServerProxy('http://localhost:8280/', use_datetime=True)

task_list = ['npxchnpak_alphav5', 'alpha_link', 'detail_alpha_link','npxchnpak_attribute', \
             'alpha_summary', 'daily_a_cov_alert', 'alpha_v5_clientb', 'daily_npxv5_load', \
             'npxchnpak_dalpha', 'daily_cpa_portfolio', 'alpha_stat_npxchnpak', 'alp_decile_npxchnpak']

#task_list = task_list[1:]

for tk in task_list:

    os.system('nipun_status --requeue=%s'%tk)
    prev_tk_st = None
    sys.stdout.write('\n\nreq %s: '%tk)
    sys.stdout.flush()
    while True:
        time.sleep(5)
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
