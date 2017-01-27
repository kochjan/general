import nipun.utils as nu

import nipun.dbc as dbc
import nipun.cpa.winsor as wins
import nipun.cpa.get_cp as get_cp
import pandas
import datetime

import nipun.utils as nu
dbo = dbc.db(connect='gce-data')

USE_NEW = True

def load_ret(date):
    if USE_NEW:
        return load_ret_new(date)
    return load_ret_q(date)

def load_data(date):
    if USE_NEW:
        return load_new_data(date)
    return load_data_q(date)


### data loaders - move me out of quandl to something else
def load_ret_q(date):

    sql = '''
    select * from dsargent.commodity_quandl_returns 
    where date ='%s'
    ''' % (date.strftime('%Y%m%d'))

    tmp =  dbo.query(sql, df=True).set_index('clean_name')
    for col in ['total', 'overnight', 'intraday']:
        tmp[col] = tmp[col].astype(float)
    return tmp

def load_ret_new(date):
    sql = '''
    select distinct
        ric  
        , overnight_return+intraday_return as total
        , overnight_return as overnight
        , intraday_return as intraday
     from dsargent.commodity_returns
    where datadate='%s'
        and 

            ric in (
                select distinct ric
                    from production_futures.futures_ref_data fr 
                    join production_futures.contract_details cd 
                        on fr.ric_root=cd.ric_root 
                where cd.future_type='commodity'
                )

    ''' % (date.strftime('%Y%m%d'))

    try:
        tmp =  dbo.query(sql, df=True).set_index('ric')
    except:
        return pandas.DataFrame()
    for col in ['total', 'overnight', 'intraday']:
        tmp[col] = tmp[col].astype(float)
    return tmp
    

def load_new_data(date):
    sql = '''
    select distinct g.*
        , overnight_return+intraday_return as total
        , overnight_return as overnight
        , intraday_return as intraday
    from production_futures.pricing_data_eod g
        left join dsargent.commodity_returns r using (datadate, ric)
    where g.datadate between '%s' and '%s'
    
            and g.ric in (
                select distinct ric
                    from production_futures.futures_ref_data fr 
                    join production_futures.contract_details cd 
                        on fr.ric_root=cd.ric_root 
                where cd.future_type='commodity'
                )
    ''' % (date-datetime.timedelta(365), date)
    data = dbo.query(sql, df=True)

    data = data.rename(columns={'last_':'close_', 'datadate':'date', 'ric': 'clean_name'})
    data['root'] = data['clean_name'].apply(lambda x: x.upper()[:-2])
    data = data.sort(['date', 'root', 'open_interest']).drop_duplicates(['date', 'root'], take_last=True)
    data['original_ric'] = data['clean_name']
    data['clean_name'] = data['root']
    return data



def run(date):
    if date.weekday() in [5,6]: return

    data = load_data(date)

    udate = date
    for col in ['total', 'overnight', 'close_', 'intraday', 'low_', 'high_', 'open_', 'volume']:
        data[col] = data[col].astype(float) 

    cut_date = date - datetime.timedelta(30)

    ### 1m momentum
    total_ret_1m = data[data['date'] > cut_date].groupby('clean_name')['total'].sum()
    total_ret_on1m = data[data['date'] > cut_date].groupby('clean_name')['overnight'].sum()

    ### 12-1m momentum
    ### 90dm
    total_ret_250 = data[data['date'] <= cut_date].groupby('clean_name')['total'].sum()
    total_ret_on250 = data[data['date'] <= cut_date].groupby('clean_name')['overnight'].sum()

    ### overnight vs intraday historical performance
    ons = data.groupby('clean_name')['overnight']
    ons_r = (ons.mean() * pandas.np.sqrt(252) / ons.std())
    ind = data.groupby('clean_name')['intraday']
    ind_r = (ind.mean() * pandas.np.sqrt(252) / ind.std())

    ### seasonality
    s_date = date - datetime.timedelta(365)
    s_date2 = s_date + pandas.datetools.BDay(20)
    seasonality = data[(data['date'] > s_date) & (data['date'] <= s_date2)].groupby('clean_name')
    seasonality_total = seasonality['total'].mean()
    seasonality_on = seasonality['overnight'].mean()

    ### clv1d 
    data['clvd'] = ((data['close_'] - data['low_']) - (data['high_'] - data['close_'])) / (data['low_'] - data['high_'])
    data['clvd'] = data['clvd'].astype(float)
    clvd_current = data[['clean_name', 'date', 'clvd']].sort('date').drop_duplicates('clean_name', take_last=True).set_index('clean_name')['clvd']
    clv = data.groupby('clean_name')
    clv1d = (clvd_current - clv['clvd'].mean()) / clv['clvd'].std()
  
    ### volatility 
    total_vol = data.groupby('clean_name')['total'].std()
    on_vol = data.groupby('clean_name')['overnight'].std()

    data['close_'] = data['close_'].astype(float)

    ### final ranks
    data = data[data['date'] == udate]

    ### liquidity ranks
    data['volume_rank'] = data['volume'].rank()
    data['oi_rank'] = data['open_interest'].rank()
    data['liq_rank'] = data[['volume_rank', 'oi_rank']].mean(axis=1)   
    
    data.set_index('clean_name', inplace=True)
    data = data[['liq_rank', 'original_ric']]
    
    data['ret1m'] = (-1.0 * total_ret_1m).rank()
    data['ret_on1m'] = (-1.0 * total_ret_on1m).rank()
    data['ret250m'] = total_ret_250.rank()
    data['ret250on'] = total_ret_on250.rank()
    data['volatili'] = total_vol.rank()
    data['overnight_rets'] = ons_r.rank()
    data['intraday_rets'] = (-1*ind_r).rank()
    data['clv1d'] = clv1d.rank()
    data['season'] = seasonality_total.rank()
    data['season_on'] = seasonality_on.rank()

    ### reindex for similarity    
#    on_vol = on_vol.reindex(data.index).dropna()
#    data = data.reindex(on_vol.index)
    if len(data) < 10:
        print data.to_string()
        print date, 'insufficient data'
        return

    data['combo'] = 0.0
    cols = {
            'ret1m': 0.1
            , 'ret250on': 0.15
            , 'volatili': 0.1
            , 'overnight_rets': 0.2
            , 'intraday_rets': 0.10
            , 'clv1d': 0.15
            , 'season_on': 0.15
    }

    for col in cols:
        if data[col].isnull().sum() == len(data):
            continue
        data['combo'] += wins.winsor(data[col])*cols[col]

    data['combo'] = wins.winsor(data['combo'])

    print data.sort('combo').to_string()
    ret_data = load_ret(date+pandas.datetools.BDay()*2)
    if len(ret_data) < 10:
        print 'insufficient return data: ', date
        return

    data['clean_ric'] = data.index
    ric_map = data[['clean_ric', 'original_ric']].copy()
    data.pop('clean_ric')
    data.set_index('original_ric', inplace=True)

    nbins = 3
    detail = pandas.merge(data, ret_data, left_index=True, right_index=True)
    detail['datadate'] = date.strftime('%Y%m%d')

    detail['bin'] = nu.qcut(detail['combo'], nbins)
    detail.to_csv('cmd_detail/%s.csv' % date.strftime('%Y%m%d'), header=True, index=True)
    return
