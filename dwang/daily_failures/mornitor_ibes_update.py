#!/opt/python2.7/bin/python2.7

import nipun.dbc as dbc
import time

dbo = dbc.db(connect='qai')
sql = """select top 10 * from qai..Update_Log where TableName in ('ibgqdetl1', 'ibgqdetl2', 'ibgqdetl3') order by StartTime DESC"""

sql2 = """select * from qai..Update_Status where TableName in ('ibgqdetl1', 'ibgqdetl2', 'ibgqdetl3')"""

while True:

    df = dbo.query(sql, df=True)
    df2 = dbo.query(sql2, df=True)

    print df
    print df2
    print "="*77

    time.sleep(60)
