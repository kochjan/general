#!/opt/python2.7/bin/python2.7

import nipun.dbc as dbc
import nipun.cpa.load_barra as lb
import datetime as dt
import pandas as pd



dbo = dbc.db(connect='qai')

sql = """
      select sm.barrid ch_barrid, sm.datadate ch_datadate, sm2.barrid p_barrid, sm2.datadate p_datadate, ohd.ValueHeld, ohd.securitycode ch_code, os.securitycode p_code, ohd.ownercode ownercode from qai..OwnHoldDet ohd
	join qai..ownsecmap os on ohd.OwnerCode=os.OwnerCode
	join nipun_prod..security_master sm on ohd.SecurityCode=sm.own_id
	and ohd.qtrdate between sm.datadate and isnull(sm.stopdate, '20500101')
	join nipun_prod..security_master sm2 on os.SecurityCode=sm2.own_id
	and ohd.qtrdate between sm2.datadate and isnull(sm2.stopdate, '20500101')
	where ohd.qtrdate ='2014-12-31 00:00:00.000' and ohd.HldgType=1

    """

df_o = dbo.query(sql, df=True)
df = df_o.sort(columns=['p_code', 'ch_code', 'ownercode', 'p_datadate', 'ch_datadate'])
df = df.drop_duplicates(cols=['p_code', 'ch_code', 'ownercode'], take_last=True)#only keep the barrids with the latest datadate
df = df.reindex(columns=['p_barrid', 'ch_barrid', 'valueheld'])
df = df.groupby(['p_barrid', 'ch_barrid']).sum().reset_index()

rsk = lb.loadrsk2('ase1jpn', 'S', dt.datetime(2014,12,31), daily=True)
rsk = rsk['USD_CAPT'].reset_index()

univ = pd.DataFrame(lb.load_production_universe('npxchnpak', dt.datetime.today() - dt.timedelta(1)).reset_index()['BARRID']).rename(columns={'BARRID':'barrid'})
df = pd.merge(df, univ, left_on=['ch_barrid'], right_on=['barrid'])#just keep ch barrids in the univ
df = pd.merge(df, univ, left_on=['p_barrid'], right_on=['barrid'])#just keep p barrids in the univ
df = pd.merge(df, rsk, left_on=['ch_barrid'], right_on=['BARRID'])

df['hld_ratio'] = df['valueheld']/df['USD_CAPT']
df[df['hld_ratio']>=0.5].to_csv('cross_holding.csv', index=None, cols=['p_barrid', 'ch_barrid', 'hld_ratio', 'valueheld', 'USD_CAPT'])


