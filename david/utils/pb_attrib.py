"""
Generate contribution for by PB for cost analysis

KN: 2016/11/17
"""

import pandas
import nipun.dbc as dbc
dbo = dbc.db(connect='gce-data')

sql = '''select ph.datadate, sign(active_weight) side, left(ph.barrid,3) as country, 
        sum({broker}_shares/ shares_held_total * contribution) as pb_contrib 
    from production_holdings.position_holdings ph join production_attribution.stock_contribution sc 
        on date_add(ph.datadate, interval 1 day)=sc.datadate and ph.barrid=sc.barrid 
    where ph.sleeve_id=1000 and ph.datadate>'20160701' 
        and sc.ra_id=1000 
    group by ph.datadate, left(ph.barrid,3) , sign(active_weight)

'''


def get_pb(broker):
    tmp = dbo.query(sql.format(broker=broker), df=True)
    tmp['broker'] = broker
    return tmp


data = pandas.DataFrame()
data = data.append(get_pb('msdw'), ignore_index=True)
data = data.append(get_pb('db'), ignore_index=True)
data = data.append(get_pb('ubs'), ignore_index=True)
data = data.append(get_pb('gs'), ignore_index=True)

ix_shorts = data['side'] == -1

shorts = pandas.pivot_table(data[ix_shorts], rows='country', cols='broker', values='pb_contrib', aggfunc=pandas.np.sum)
longs = pandas.pivot_table(data[-ix_shorts], rows='country', cols='broker', values='pb_contrib', aggfunc=pandas.np.sum)

data = pandas.merge(shorts, longs, left_index=True, right_index=True, suffixes=('_short', '_long'), how='outer')
print data



