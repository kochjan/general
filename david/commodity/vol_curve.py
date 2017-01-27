
import datetime
import pandas
import nipun.dbc as dbc
dbo = dbc.db(connect='gce-data')

blocks = {
        'NG': '''case when timestamp between '17:00:00' and '17:59:00' then 0 else volume end''',
        'KC': 'volume',
        'LH': 'volume',
        'HO': 'volume',
        'CT': 'volume',  
        'PL': 'volume',
        'GC': '''case when timestamp between '17:00:00' and '17:59:00' then 0 else volume end''',
        'SI': '''case when timestamp between '17:00:00' and '17:59:00' then 0 else volume end'''
}        

contract_val = {
    'KC': '{price}/100.0*37500', 
    'PB': '{price}/100.0*40000', 
    'C': '{price}/100.0*5000', 
    'LB': '{price}/100.0*110000', 
    'LC': '{price}/100.0*40000', 
    'CL': '{price}*1000', 
    'CC': '{price}*10', 
    'S': '{price}/100.0*5000', 
    'LH': '{price}/100.0*40000', 
    'NG': '{price}*10000', 
    'HO': '{price}*42000', 
    'SI': '{price} * 5000', 
    'FC': '{price}/100.0*50000', 
    'GC': '{price}*100', 
    'O': '{price}/100.0*5000', 
    'W': '{price}/100.0*5000', 
    'SB': '{price}/100.0*112000', 
    'CT': '{price}/100.0*50000', 
    'PL': '{price}*50', 
    'OJ': '{price}/100.0*15000'
}



def run_commod(ric_root):


    repls = {'ric': '%s%%' % ric_root,  
            'riclen': len(ric_root)+2,
            'blockout': blocks.get(ric_root, 'volume')
    }

    sql = '''
    select 
        datadate
        , timestamp
        , {blockout} as volume
        , no_trades
        , last_ as price
     from dsargent.commodity_intraday_v2
    where
        ric like '{ric}'
        and length(ric) = {riclen}
        and datadate>'20130101'
        and last_ is not null 
    '''.format(**repls)

    data = dbo.query(sql, df=True)
    
    days_total_vol = pandas.DataFrame(data.groupby('datadate')['volume'].sum(), columns=['total_volume'])
    interval_vol = data.groupby(['datadate', 'timestamp'])['volume'].sum().reset_index()
    price = pandas.DataFrame(data.groupby(['datadate', 'timestamp'])['price'].mean().reset_index())

    data = pandas.merge(interval_vol, days_total_vol, left_on='datadate', right_index=True)
    data = pandas.merge(data, price, on=['datadate', 'timestamp'])

    data['pct_vol'] = data['volume'] / data['total_volume'].astype(float)
    data['date'] = data['timestamp'].apply(lambda x: datetime.datetime(2016,1,1)+data['timestamp'])
    
    notional_calc = lambda x: eval(contract_val[ric_root].replace('{price}',str(x)))
    data['contract_size'] = data['price'].map(notional_calc) * 1
    data['daily_notional'] = data['contract_size'] * data['volume'] / 1e6

    avg_vol = data.groupby('date')['pct_vol'].mean()
    med_vol = data.groupby('date')['pct_vol'].median()
    med_notional = data.groupby('date')['daily_notional'].median()
    avg_notional = data.groupby('date')['daily_notional'].mean()

    outdata = pandas.DataFrame({'avg_vol': avg_vol, 'med_vol': med_vol, 'med_notional': med_notional, 'avg_notional': avg_notional})
    return outdata
