#!/home/wzhu/anaconda/bin/python

# gather the CPA factor return results

# *portrets.csv contains forward returns:
#  RET-K  means today's portfolio's return K-days from now

# input condition and values:
# past (days or week) portfolio signal return (using RET0 cumulative gains from past K days)
# past portfolio barra factor return

# try to predict: future signal return (using RET-0 ... RET-K)
import pandas as pd
import numpy as np
import pickle

WINDOW_DAYS = 5  #weekly
LOOKBACKMONTH = 20

basedir = '/home/wzhu/research/ml/b.sig_timing/npxchnpak/'
path = basedir + '{SIG}/{CTY}_{SIG}_20070424_20161231_portrets.csv'

SIGNALS = ['b2p_in', 'bpbeta','cratio_e','e12p_in','e2p_in','lowsrisk']
FACTORS = ['growth', 'leverage', 'liquidit', 'momentum', 'size', 'value', 'volatili']


def prepare_country_fret_df(cty):
    """
    :param cty: country of interest
    :return: DF with index=date, columns=past-K-day-cumRET0 factor mean and std, future K-day fret
    """
    dfs = {}
    for sig in SIGNALS+FACTORS:
        print cty, sig
        df = pd.read_csv(path.format(CTY=cty, SIG=sig),
                 index_col=0, parse_dates=True)
        #label = '{}.{}'.format(sig,cty)
        #dfs[label] = df
        dfs[sig] = df
    panel_data = pd.Panel(dfs)

    # darn, extra columns:
    # RET-K = return
    # LOG-K = log(ret-k) normalize to risk=20%annual
    # CUMLOG-K = cum of above normalized log
    #
    # for simplicity we will just pick the log-K and sum them to get future 20-day return

    # This computes the future 20 day factor return
    #logcols = [x for x in panel_data.minor_axis if x.startswith('LOG') and x != 'LOG0']
    logcols = ['LOG{}'.format(x) for x in range(1, 1 + WINDOW_DAYS)]
    logdata = panel_data.ix[:,:,logcols]
    logsum=logdata.sum(axis=2)
    logsum.columns = [x+'.fret' for x in logsum.columns]
    fret = logsum.applymap(lambda x: np.exp(x)-1)

    # this computes the past 20 day cumulative RET0
    # requires new version of anaconda for rolling
    ret0 = panel_data.ix[:,:,'RET0']
    ret0na = ret0.dropna()
    retweek= ret0na.rolling(WINDOW_DAYS)
    retmonth = ret0na.rolling(LOOKBACKMONTH)
    ret_week_sum = retweek.sum().dropna()
    ret_month_std = retmonth.std().dropna()
    col = ret0.columns
    ret_month_std.columns = [x+'.std' for x in col]
    ret_week_sum.columns = [x+'.sum' for x in col]
    ret_past = ret_week_sum.join(other=ret_month_std, how='inner')

    # combine the future fret and past RET0:
    alldata = ret_past.join(other=fret, how='inner')

    return alldata


CTYS = ['TWN','KOR', 'JPN', 'HKGCHX', 'AUS']
panel_df = {}
for country in CTYS:
    print country
    df = prepare_country_fret_df(country)
    print df.head(2).T
    panel_df[country] = df
allpanel = pd.Panel(panel_df)

with open('country_fret_panel.{}'.format(WINDOW_DAYS), "wb") as outfh:
    pickle.dump(allpanel, outfh)

#with open('country_fret_panel','rb') as infh:
#    allpanel=pickle.load(infh)
