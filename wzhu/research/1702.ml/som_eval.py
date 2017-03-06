#!/opt/anaconda/bin/python
import numpy as np
import pandas as pd
import datetime
import pickle
import som
import os
import sys

from scipy import stats

import glob
def get_forecast(path):
    rall = None
    for f in glob.glob(path):
        df = pd.read_csv(f, index_col=0, parse_dates=['date'])
        if rall is None:
            rall = df
        else:
            rall = rall.append(df)
    return rall

SIGNALS = ['b2p_in', 'bpbeta', 'cratio_e', 'e12p_in', 'e2p_in', 'lowsrisk']
BARRA =  ['growth', 'leverage', 'liquidit', 'momentum', 'size', 'value', 'volatili']

def eval_strategy_with_shift(eval_data, week_delay):
    eval_data2 = eval_data[ [x for x in eval_data.columns if x.endswith('.fret')] ]
    for s in SIGNALS:
        eval_data2[s+'.sgn'] =  eval_data2['pred_'+s+'.fret'].apply(lambda x: 1 if x > 0 else 0)

    eval_data2['total.sgn'] = eval_data2[ [s+'.sgn' for s in SIGNALS] ].sum(axis=1)

    base_weight = 1.0/len(SIGNALS)
    for s in SIGNALS:
        eval_data2[s+'.wt'] = eval_data2[s+'.sgn']/eval_data2['total.sgn']
        eval_data2[s+'.prevwt'] = eval_data2[s+'.wt'].shift(week_delay)
        # can also test the more realistic shift=2
        eval_data2[s+'.contr_base'] = eval_data2[s+'.fret']*base_weight
        eval_data2[s+'.contr'] = eval_data2[s+'.fret'] * eval_data2[s+'.prevwt']

    eval_data2=eval_data2.dropna()    
    eval_data2['total.contr'] = eval_data2[ [s+'.contr' for s in SIGNALS] ].sum(axis=1)
    eval_data2['total.contr_base'] = eval_data2[ [s+'.contr_base' for s in SIGNALS] ].sum(axis=1)
    eval_data2['total.contr_diff'] =  eval_data2['total.contr'] - eval_data2['total.contr_base']
    #print eval_data2.head(2).T
    return eval_data2


if __name__=='__main__':
    dir = sys.argv[1]
    if not os.path.exists(dir):
        print 'dir not exists:', dir
        sys.exit(0)

    eval_data = get_forecast(dir+'/*.csv')

    strat_delay = [None for x in range(10)]
    # to compare fairly, we will use the common last 250 for these
    for w in range(10):
        strat_delay[w] = eval_strategy_with_shift(eval_data, w).tail(200)
    #eval_data2[['total.contr','total.contr_base']].cumsum().plot()

    delay_perf = strat_delay[0][['total.contr','total.contr_base','total.contr_diff']].describe().rename(
        columns={'total.contr':'contr0','total.contr_base':'base','total.contr_diff':'diff0'})
    for x in range(9):
        delay_perf = delay_perf.join(strat_delay[x+1][['total.contr','total.contr_diff']].describe().rename(
            columns={'total.contr':'contr{}'.format(x+1), 'total.contr_diff':'diff{}'.format(x+1)}))

    delay_perf_T = delay_perf.T
    delay_perf_T['Tstat']=delay_perf_T['mean']/delay_perf_T['std']
    delay_perfT = delay_perf_T.T

    print delay_perf_T[['Tstat']]

