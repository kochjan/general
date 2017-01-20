
import pandas
import datetime
import nipun.dbc as dbc
import nipun.trading_days as td
import nipun.cpa.load_barra as lb
import nipun.returns as rets
import nipun.volume as vol

import nipun.cloud.data as gdata

dbo = dbc.db(connect='qai')
RET_WINDOW = 12
USE_RET = 'RET'
PMOM_BREAK = [1, 3, 5, 10, 20]
VOL_WINDOW = 60
STVOL_WINDOW = 10

SIGNALS = ['b2proe', 'roa_fast', 'fcf_oa_fast', 'updn2midc', 'stvol', 'stliq', 'd2p', 'e2p', 'pmom','price_target', 'e12p_tsv', 'cratio_e']
VERSION = 'ml-base-test1'

def load_signals(date):

    pth = '/research/production_alphas/daily/current/npxchnpak/{a}/{a}_{dt}.alp'
    pth2 = '/research/production_alphas/daily/live/{a}/{a}_{dt}.alp'

    data = pandas.DataFrame()
    missing = []
    for s in SIGNALS:
        try:
            tmp = pandas.read_csv(pth.format(a=s, dt=date.strftime('%Y%m%d')), names=['barrid', s], index_col=0)
            data = pandas.merge(data, tmp, left_index=True, right_index=True, how='outer')
        except:
            try:
                tmp = pandas.read_csv(pth2.format(a=s, dt=date.strftime('%Y%m%d')), names=['barrid', s], index_col=0)
                data = pandas.merge(data, tmp, left_index=True, right_index=True, how='outer')
            except:
                print 'missing: ', s, date
                missing.append(s)

    for s in missing:
        data[s] = pandas.np.nan
   
    ### note: don't fill with 0 yet as we will winsorize and then fill in with 0s 
    return data


def gen_fwd_returns(data, date, fret=None):
    
    window = td.get_trading_dates(date, 1, fdays=20)
    data = td.filter_df(data, 'DATADATE', 'BARRID')
    data = data[data['dt_idx'] != 't0']
    
    data['ndays'] = data['dt_idx'].apply(lambda x: int(x.replace('f','')))
    data['ndays'] = data['ndays'].astype(int)
    data['logret'] = pandas.np.log(1+data[USE_RET])

    frets = pandas.DataFrame()

    for pm in fret:
        ix = data['ndays'] <= pm
        tmp = data[ix].groupby('BARRID')['logret'].sum()
        tmp = pandas.DataFrame(tmp, columns=['FWD_%s_%s' % (USE_RET, pm)])
        frets = pandas.merge(frets, tmp, left_index=True, right_index=True, how='outer')
    
    return frets 
    

def gen_pmom_returns(data, date, pmom=None):

    window = td.get_trading_dates(date, RET_WINDOW)

    data = td.filter_df(data, 'DATADATE', 'BARRID')
    data['ndays'] = data['dt_idx'].apply(lambda x: int(x.replace('t','')))
    data['ndays'] = data['ndays'].astype(int)
    out_pmom = pandas.DataFrame(data.groupby('BARRID')[USE_RET].std(), columns=['volatility'])
    data['logret'] = pandas.np.log(1+data[USE_RET])
    
    for pm in pmom:
        ix = data['ndays'] <= (pm-1)
        tmp = data[ix].groupby('BARRID')['logret'].sum()
        tmp = pandas.DataFrame(tmp, columns=['%s_%s' % (USE_RET, pm)])
        out_pmom = pandas.merge(out_pmom, tmp, left_index=True, right_index=True, how='outer')

    return out_pmom


def gen_vol(data, date, window):

    _ = td.get_trading_dates(date, VOL_WINDOW)
    data = td.filter_df(data, 'datadate', 'BARRID')
    data['ndays'] = data['dt_idx'].apply(lambda x: int(x.replace('t','')))
    data['ndays'] = data['ndays'].astype(int)
    data = data[['ndays', 'BARRID', 'USDDolVol']]
    stvol = data[data['ndays'] < window].copy()
    stvol_hist = data[data['ndays'] >= window].copy()

    stvol = (stvol.groupby('BARRID')['USDDolVol'].mean() - stvol_hist.groupby('BARRID')['USDDolVol'].mean()) \
        / (stvol_hist.groupby('BARRID')['USDDolVol'].std())

    stvol = pandas.DataFrame(stvol, columns=['stvol_%s' % window])
    return stvol

def run(date):

    ### load forward returns
    retdata = rets.daily_resrets(date+pandas.datetools.BDay(30), lookback=60+RET_WINDOW)
    fret = gen_fwd_returns(retdata.copy(), date, [5, 10, 20])
    exrets = gen_pmom_returns(retdata.copy(), date, pmom=PMOM_BREAK)

    rsk = lb.loadrsk2('ase1jpn', 'S', date, daily=True)
    rsk = rsk[['COUNTRY', 'USD_CAPT']]

    ### load returns compute residual cumulative
    ret = rets.daily_resrets(date, lookback=RET_WINDOW)

    ### load volume and signal data
    voldata = vol.load_volume(date, window=VOL_WINDOW)
    voldata = gen_vol(voldata, date, STVOL_WINDOW)
    signals = load_signals(date)

    data = pandas.merge(exrets, voldata, left_index=True, right_index=True, how='inner')
    data = pandas.merge(data, signals, left_index=True, right_index=True, how='inner')
    data = pandas.merge(data, fret, left_index=True, right_index=True, how='inner')
    data = pandas.merge(data, rsk, left_index=True, right_index=True, how='inner')
    data['datadate'] = date
   
    univ = lb.load_production_universe('npxchnpak', date)
    data = data[data.index.isin(univ.index)]
    print len(data) 
    gdata.write_gce(data, 'users', 'dsargent/{version}/{dt}.pd'.format(version=VERSION,dt=date.strftime('%Y%m%d')), enable_compression=True)
    return 
