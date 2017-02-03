
import datetime
import pandas
import nipun.dbc as dbc
dbo = dbc.db(connect='qai')

def run(ident):

    sql = '''
    select 
    datadate
    , stopdate

    , barrid
    , active_flag
    , cusip
    , ds2_id
    , dxl_id
    , ibes_code
    , ibes_region
    , isin
    , isocurr
    , listed_country
    , localid
    , name
    , repno
    , sedol
    , synth_barrid
    , tk2_id
    , wspit_id

    , own_id

    from nipun_prod..security_master where barrid='%s' 

    ''' % ident

    data, heads = dbo.query(sql, headers=True)

    heads = dict(zip(heads, xrange(len(heads))))

    date_getter = lambda x: (x[0], x[1])
    own_getter = lambda x: x[-1]

    secm_data = lambda x: [x[i] for i in range(2, len(x)-1)]


    prev_key = None
    prev_date = None
    prev_own = None

    keep_rows = []

    for row in data:

        date = date_getter(row)
        primkey = secm_data(row)
        optional = own_getter(row)

        if primkey != prev_key:
            keep_rows.append(row)
            prev_key = primkey 
            prev_own = optional
    
        elif primkey == prev_key and optional==None and prev_own!=None:
            keep_rows.append(row)
            prev_own = optional
            prev_key = primkey 
            
        elif primkey == prev_key and optional!=None and prev_own==None:
            keep_rows[-1][1] = date[1]

    
    cols = ['datadate', 'sedol', 'barrid', 'cusip', 'isin', 'localid', 'name', 'listed_country', 'isocurr', 'dxl_id', 'ds2_id', 'wspit_id', 'tk2_id', 'ibes_region', 'ibes_code', 'synth_barrid', 'stopdate', 'active_flag','own_id', 'repno']
    getter = lambda x: [x[heads[cname]] for cname in cols]

    outputs = []
    for row in keep_rows:
        out = getter(row)
        outputs.append(out)

    data = pandas.DataFrame(outputs, columns=cols)
    data['datadate'] = data['datadate'].apply(lambda x: x.strftime("%Y%m%d"))
    ix = data['stopdate'].notnull()
    data['stopdate'][ix] = data['stopdate'][ix].apply(lambda x: x.strftime('%Y%m%d'))
    data.to_csv('secmupd.csv', index=False, header=False, mode='a')



def queuer():

    sql = '''
    select distinct barrid, count(*) from nipun_prod..security_master sm group by barrid having count(*) > 20
    '''
    
    data = dbo.query(sql, df=True)
    ids = data['barrid'].tolist()

    map(run, ids)
    
queuer()

