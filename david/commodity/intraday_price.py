
import nipun.dbc as dbc
import pandas
import datetime

dbo = dbc.db(connect='gce-data')


def runric(ricroot, daytime):

    riclen = len(ricroot)+2

    sql = '''select datadate, timestamp, ric, open_, last_, volume 
        from dsargent.commodity_intraday_v2 where ric like '%s%%'  and length(ric)=%s
        and datadate between '20120101' and '20160730'
    ''' % (ricroot, riclen)

    data = dbo.query(sql, df=True)
    maxvol = data.groupby(['ric', 'datadate'])['volume'].sum().reset_index()
    maxvol = maxvol.sort(['datadate', 'volume'])
    maxvol = maxvol.drop_duplicates(['datadate'], take_last=True)
    data = pandas.merge(data, maxvol, on=['ric', 'datadate'], how='inner')

    data['timestamp'] = data['timestamp'].apply(lambda x: (datetime.datetime.min + x).time())

    data = data.sort(['datadate', 'timestamp'])
    ix_sod = data['timestamp'] == daytime
    
    data['sod_price'] = pandas.np.nan
    data['sod_ric'] = None
    data['sod_price'][ix_sod] = data['last_'][ix_sod].astype(float)
    data['sod_ric'][ix_sod] = data['ric'][ix_sod]

    data['sod_price'] = data['sod_price'].fillna(method='ffill')
    data['sod_ric'] = data['sod_ric'].fillna(method='ffill')
    data = data[data['ric'] == data['sod_ric']]

    data['intraday_return'] = data['last_'] / data['sod_price'] - 1.0
    data['intraday_return'] = data['intraday_return'].astype(float) 
    data = data[data['volume.x'] != 0]
    data = data[data['intraday_return'].notnull()]

    valid_times = data.groupby('timestamp')['intraday_return'].count()
    ix_valid = valid_times > (valid_times.mean() * 0.1)

    if ricroot in ['SI']:    
        ### erroneous prints and rebasiing...
        data = data[data['intraday_return'].abs() < 0.5]

    res = data.groupby('timestamp')['intraday_return'].mean()
    res = res.ix[ix_valid]
    res.index =  [datetime.datetime.combine(datetime.datetime.today(), x) for x in res.index]


    if ricroot in ['BO', 'W', 'KW']:

        stoptime = datetime.time(14, 20)
        starttime = datetime.time(19, 0)

        ix1 = datetime.datetime.combine(datetime.datetime.today(), stoptime)
        ix2 = datetime.datetime.combine(datetime.datetime.today(), starttime)
        ix = [(x <= ix1) or (x >= ix2) for x in res.index]
        res = res.ix[ix]


        

    return res, data


