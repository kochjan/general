import nipun.dbc as dbc
import nipun.cpa.load_barra as lb
import pandas

dbo = dbc.db(connect='gce-reporting')
def run(date):

    sql = '''
    select * from production_holdings.prelocate_request where datadate='%s'
    and shares_received is not null
    ''' % date.strftime('%Y%m%d')

    data = dbo.query(sql, df=True)
    if data is None:
        return
    rsk = lb.loadrsk2('ase1jpn', 'S', date, daily=True)
    rsk['usdp'] = rsk['USD_CAPT'] * rsk['LOC_PRIC'] / rsk['LOC_CAPT']
    data = pandas.merge(data, rsk[['usdp']], left_on='barrid', right_index=True, how='left')
    data['request_notional'] = data['usdp'] * data['shares_requested'].astype(float)
    data['get_notional'] = data['usdp'] * data['shares_received'].astype(float)
    data['delta_notional'] = data['request_notional'] - data['get_notional']

    a = data[['request_notional', 'get_notional', 'delta_notional']].sum() / 1e6
    a = pandas.DataFrame(a)
    a = a.reset_index()
    
    a['datadate'] = date.strftime('%Y%m%d')
    a = a.pivot(index='datadate', columns='index', values=0)
    a['fill_pct'] = a['get_notional'] / a['request_notional']
    print a.to_string()
