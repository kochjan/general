
import pandas
import nipun.dbc as dbc
import datetime
import nipun.sql_io as sl

dbo = dbc.db(connect='gce-data')

PRICE = 'dsargent.commodity_intraday_v2'
REF = 'dsargent.commodity_refdata_v3'

price_windows = [
    ('W', 5, 30),
    ('KW', 5, 30),
    ('C', 5, 20),
    ('S', 5, 20),
    ('SM', 5, 20),
    ('BO', 5, 20), 
    ('SB', 10, 5),
    ('CC', 5, 10),
    ('KC', 5, 10),
    ('CT', 5, 10),
    ('GC', 5, 30),
    ('SI', 5, 30),
    ('PL', 5, 30),
    ('HG', 5, 30),
    ('PA', 5, 30),
    ('CL', 5, 30),
    ('LCO', 5, 30),
    ('HO', 5, 30),
    ('NG', 5, 30),
    ('LGO', 5, 30),
    ('LH', 5, 15),
    ('LC', 5, 15),
    ('FC', 5, 15)
]

#price_windows = [('SB', 2, 2)]

def load_meta(ric):
    sql = '''
    select * from {ref}
    where ric='{ric}' 
    '''.format(ref=REF, ric=ric)
    return dbo.query(sql, df=True)

def load_data(ric, buy_nminute, sell_nminute):


    date_logic = ' case when ref.buy_time <  ref.sell_time then 1 else 0 end as same_day'
    buy_logic = '''
        case when cm.timestamp between ref.sell_time and date_add(ref.sell_time, interval {sell_nminute} minute) then 'open' 
            when cm.timestamp between date_sub(ref.buy_time, interval {buy_nminute} minute) and ref.buy_time then 'close'
            else 'uhoh' end as timeprint
    '''

    repls = {
        'cmd_pricing': PRICE,
        'cmd_ref': REF,
        'ric': ric+'%%',
        'ric_root': ric,
        'buy_nminute': buy_nminute,
        'sell_nminute': sell_nminute,
        'date_logic': date_logic,
        'ric_size': len(ric)
    }

    repls['buy_logic'] = buy_logic.format(**repls) 

    sql = '''
    select  
        cm.ric
        , cm.datadate                
        , {buy_logic}
        , {date_logic}
        , sum(volume) as total_volume
        , sum(coalesce(vwap, avg_price) * volume) / sum(volume) as interval_price

        from {cmd_pricing} cm
        join {cmd_ref} ref

    on cm.ric like '{ric}'
        and ref.ric='{ric_root}'
        and left(cm.ric, {ric_size}) = ref.ric
        and length(cm.ric) = ({ric_size}+2)

    where 1=1
        and last_ is not null
    and datadate>'20110101'
    group by cm.ric, cm.datadate, timeprint
    having timeprint!='uhoh'
    
    '''.format(**repls)
    
    data = dbo.query(sql, df=True)
    sd = data['same_day'].unique().tolist()
    if len(sd) != 1: raise
    sd = sd[0]



    data = compute_returns(data)

#    data.set_index('datadate', inplace=True)
    
    refdata = load_meta(ric)
    cvalue = refdata['contract_value'].values[0]
    
    notional_calc = lambda x: eval(cvalue.replace('{price}',str(x)))

    ix = data['open_price'].notnull()
    data['contract_size'] = pandas.np.nan
    data['contract_size'][ix] = data['open_price'][ix].map(notional_calc) * 1
    
    data['daily_notional'] = data['contract_size'] * data['open_volume'] / 1e6

    if sd:
        data = data[['datadate', 'ric', 'prev_open', 'close_price', 'overnight_return', 'intraday_return', 'close_volume', 'prev_open_volume']]
        data = data.rename(columns={'prev_open': 'open_price', 'prev_open_volume':'open_volume'})
    else:
        data = data[['datadate', 'ric', 'open_price', 'close_price', 'overnight_return', 'intraday_return', 'close_volume', 'open_volume']]
    
    data = data.dropna().reset_index(drop=True)
    sl.write_frame(data, 'dsargent.commodity_returns_v3', if_exists='append', user='gce-data')
    return


def compute_returns(data, pick_priority=1):

    data = data.sort(['ric', 'datadate'])
    for col in ['interval_price', 'total_volume']:
        data[col] = data[col].astype(float)

    sd = data['same_day'].unique().tolist()

    if len(sd) != 1: raise
    prices = pandas.pivot_table(data, rows=['ric', 'datadate'], cols='timeprint', values='interval_price')
    volumes = pandas.pivot_table(data, rows=['ric', 'datadate'], cols='timeprint', values='total_volume')
    data = pandas.merge(prices, volumes, left_index=True, right_index=True, suffixes=('_price', '_volume'))

    data = data.reset_index()
    data = data.sort(['ric', 'datadate'])


    if sd[0] == 1:
        data['prev_open'] = data['open_price'].shift(1)
        data['prev_open_volume'] = data['open_volume'].shift(1)
        data['prev_close'] = data['close_price'].shift(1)
        data['prev_ric'] = data['ric'].shift(1)
        data = data[data['prev_ric'] == data['ric']]

        ix = data['prev_close'].isnull()
        data['prev_ric2'] = data['prev_ric']
        data['prev_ric2'][ix] = pandas.np.nan
        data['prev_close'] = data['prev_close'].fillna(method='ffill')
        data['prev_ric2'] = data['prev_ric2'].fillna(method='ffill')
         
        data = data[data['prev_ric2'] == data['ric']]
        data['overnight_return'] = data['prev_open'] / data['prev_close'] - 1.0
        data['intraday_return'] = data['close_price'] / data['prev_open'] - 1.0
        data.pop('prev_ric')

        ix = data['prev_open_volume'].notnull() & data['close_volume'].notnull()
        data = data[ix]
        data = data.sort(['datadate', 'prev_open_volume'])

    else:
        data['lag_close'] = data['close_price'].shift(1)
        data['lag_ric'] = data['ric'].shift(1)
        data = data[data['lag_ric'] == data['ric']]
        data['overnight_return'] = data['open_price'] / data['lag_close'] - 1.0
        data['intraday_return'] = data['close_price'] / data['open_price'] - 1.0
        data.pop('lag_ric')

        ix = data['open_volume'].notnull() & data['close_volume'].notnull()
        data = data[ix]
        data = data.sort(['datadate', 'open_volume'])

    return data

    for k in range(pick_priority):
        tmp = data.drop_duplicates(['datadate'], take_last=True)
        if k+1 == pick_priority:
            return tmp
        data = pandas.merge(data, tmp[['ric', 'datadate', 'close_price']], how='left', on=['ric', 'datadate'], suffixes=('', '_chk'))
        data = data[data['close_price_chk'].isnull()]
        data.pop('close_price_chk')
        data = data.sort(['datadate', 'open_volume'])
    return data
    
for c, buy, sell in price_windows:
    load_data(c, buy, sell)
    print c, buy, sell
