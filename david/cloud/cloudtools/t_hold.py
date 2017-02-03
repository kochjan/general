
import nipun.dbc as dbc
dbo = dbc.db(connect='g_holdings')

def run(date):

    data = dbo.query('''select * from production_holdings.position_holdings where sleeve_id=1000 and datadate='%s' 
    ''' % date, df=True)
    print date
    print data[['total_notional_usd', 'active_weight']].abs().sum()

