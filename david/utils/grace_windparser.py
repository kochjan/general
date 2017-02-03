import re
import pandas
import nipun.dbc as dbc

dbo = dbc.db(connect='qai')
data = dbo.query('''SELECT PUBLISHDATE, WINDCODES
 FROM [wind_data].[dbo].[STOCKPOSITIVENEWS]
 order by PUBLISHDATE''', df=True)

parser = lambda x:  re.findall(r'[0-9]{6}.S[H,Z]', x)

data['codes'] = None
ix = data['windcodes'].notnull()
data['codes'][ix] = data['windcodes'][ix].apply(parser)

outdata = []
for i, row in data[ix].iterrows():
    outdata.extend([row['publishdate'], x] for x in row['codes'])

final_data = pandas.DataFrame(outdata, columns=['date', 'ticker'])
print final_data.head()

