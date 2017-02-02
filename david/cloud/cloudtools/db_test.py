import pandas
import nipun.dbc as dbc

dbo = dbc.db(connect='gce-data')

def run(date):

    sql = '''
    select * from
    nipun_prod.broker_gini_data
    where datadate='%s'
    ''' % date

    data = dbo.query(sql, df=True)
    print data.head(20).to_string()

#    ca.write_alpha(data, 'cjs_alpha', 'npxchnpak', date)

