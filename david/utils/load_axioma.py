import os
import gzip
import pandas
import zipfile as zf
from cStringIO import StringIO
import nipun.dbc as dbc
dbo = dbc.db(connect='qai')

OUTPATH = '/opt/data/axioma/{model}/daily/{yr}/' 
VALID_MODELS = ['SH-S', 'MH-S', 'SH', 'MH']
ZIPARCH = '/home/sil/production/outputs/'

def __load_barra_mapping(date):
    '''load barra-> sedol mapping for crossing over axioma ids'''
    sql = '''
    select distinct barrid, sedol, datadate from nipun_prod..security_master 
    where '%s' between datadate and isnull(stopdate, '20500101')
    and barrid is not null and sedol is not null
    ''' % (date.strftime('%Y%m%d'))

    data = dbo.query(sql, df=True)
    data = data.sort(['sedol', 'datadate']).drop_duplicates(['sedol'], take_last=True)
    data = data.drop_duplicates(['barrid'], take_last=True)
    
    return data[['barrid', 'sedol']]

def __read_exp(pth):
    ''' read raw exposure files '''
    data = pandas.read_csv(pth, skiprows=[0,1,3,4], sep='|')
    remap = {data.columns.tolist()[0]:'AxiomaID', 'SEDOL(7)':'sedol'}
    data = data.rename(columns=remap)
    cash_assets = filter(lambda x: x.startswith('CSH_'), data['AxiomaID'])
    data = data[~data['AxiomaID'].isin(cash_assets)]
    return data

def __read_idm(pth):
    ''' read raw identifier files '''
    
    data = pandas.read_csv(pth, skiprows=[0,1], sep='|')
    remap = {data.columns.tolist()[0]:'AxiomaID', 'SEDOL(7)':'sedol'}
    data = data.rename(columns=remap)
    return data[['AxiomaID', 'sedol']]

def __read_cov(pth):
    ''' read raw covariance files '''
    data = pandas.read_csv(pth, skiprows=[0,1], sep='|')
    remap = {data.columns.tolist()[0]:'FactorName'}
    data = data.rename(columns=remap)
    return data

def __read_ret(pth):
    ''' read raw factor return files '''
    data = pandas.read_csv(pth, skiprows=[0,1], sep='|')
    remap = {data.columns.tolist()[0]:'FactorName'}
    data = data.rename(columns=remap)
    return data

def __read_rsk(pth):
    ''' read raw rsk files '''
    data = pandas.read_csv(pth, skiprows=[0,1,3,4], sep='|')
    remap = {data.columns.tolist()[0]:'AxiomaID'}
    for col in data.columns:
        if col not in remap:
            remap[col] = col.replace(' ','_').lower()
    
    data = data.rename(columns=remap)
    ix = data['estimation_universe'] == '*'
    data['estimation_universe'].ix[ix] = 1
    for col in ['estimation_universe', 'estimation_universe_weight']:
        data[col] = data[col].fillna(0)

    data['estimation_universe'] = data['estimation_universe'].astype(int)
    return data

def __generate_ax_files(model, date, use_flatfile=False):
    '''map all axioma files'''

    files = ['exp', 'rsk', 'cov', 'idm', 'ret']
    barra_data = __load_barra_mapping(date)
    risk_model = {}

    if use_flatfile:
        ### if running historical and need access to flatfiles
        source_path = '/mnt/nipunsql/axioma-data/flatfile/{yr}/{mth}/AXAP21-{model}.{date}.{ftype}'
#        source_path = '/mnt/nipunsql/flatfile/{yr}/{mth}/AXAP21-{model}.{date}.{ftype}'
    
        for ftype in files:
            _tmp = source_path.format(yr=date.strftime('%Y'), mth=date.strftime('%m'), model=model, date=date.strftime('%Y%m%d'), ftype=ftype)
            risk_model[ftype] = eval('__read_{ftype}(_tmp)'.format(ftype=ftype))

    else:
        ### otherwise use the daily zip archive
        zip_arch = '{path}/AXAP21-{model}.{date}.zip'
        repls = {'path':ZIPARCH, 'model':model, 'date': date.strftime('%Y%m%d')}
        zipfile = zf.ZipFile(zip_arch.format(**repls), 'r')

        for ftype in files:
            repls['ftype'] = ftype
            _tmp = StringIO(zipfile.read('AXAP21-{model}.{date}.{ftype}'.format(**repls)))
            risk_model[ftype] = eval('__read_{ftype}(_tmp)'.format(ftype=ftype))
            

    ### map over to barrid - drop missing barrids from dataset
    if 'idm' in files and 'exp' in files:
        risk_model['idm'] = pandas.merge(risk_model['idm'], barra_data, on='sedol', how='left')
        risk_model['exp'] = pandas.merge(risk_model['exp'], risk_model['idm'], on='AxiomaID')
        risk_model['exp'] = risk_model['exp'].drop(['sedol', 'AxiomaID'], axis=1)
        ix = risk_model['exp']['barrid'].notnull()
        risk_model['exp'] = risk_model['exp'][ix]
        risk_model['exp'].set_index('barrid', inplace=True)
        risk_model['exp'] = risk_model['exp'].sort()

    if 'idm' in files and 'rsk' in files:
        risk_model['rsk'] = pandas.merge(risk_model['rsk'], risk_model['idm'], on='AxiomaID')
        risk_model['rsk'] = risk_model['rsk'].drop(['sedol', 'AxiomaID'], axis=1)
        ix = risk_model['rsk']['barrid'].notnull()
        risk_model['rsk'] = risk_model['rsk'][ix]
        risk_model['rsk'].set_index('barrid', inplace=True)
        risk_model['rsk'] = risk_model['rsk'].sort()

    ### output files
    outpath = OUTPATH.format(model=model, yr=date.strftime('%Y'))
    if not os.path.exists(outpath):
        try:
            os.makedirs(outpath)
        except Exception, e:
            raise ValueError('error creating directory %s' % outpath)
       
    for ftype in risk_model:
        tmp = StringIO() 
        risk_model[ftype].to_csv(tmp, index=True, header=True)
        fh = gzip.open(outpath+'{date}.{ftype}.gz'.format(date=date.strftime('%Y%m%d'), ftype=ftype), 'wb')
        fh.write(tmp.getvalue())
        fh.close()

    return


def validate_model(func):
    '''wrapper to check model input'''
    def wrapper(model, *args):
        if model.upper() not in VALID_MODELS:
            raise AttributeError('model must be one of %s' % VALID_MODELS)
        return func(model.upper(), *args)

    ### pass through documentation and name data for help calls
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    return wrapper
     
def __loader(model, date, ftype):
    '''
    generic data loader that knows how to read axioma compressed risk filse

    returns DataFrame
    '''
    pth = OUTPATH.format(model=model, yr=date.strftime('%Y'))
    raw = StringIO(gzip.GzipFile(pth+'{date}.{ftype}.gz'.format(date=date.strftime('%Y%m%d'), ftype=ftype), 'r').read())
    data = pandas.read_csv(raw, index_col=0)
    return data 

@validate_model
def loadfret(model, date):
    ''' 
    load axioma factor returns data
    warning: for stat factors these may be hard to 
        interpret without looking at exposures
    
    Parameters:
    model : str
        name of axioma risk model
    date : datetime
        date of axioma risk model

    returns DataFrame
    '''
   
    tmp = __loader(model, date, 'ret') 
    tmp['datadate'] = date
    return tmp

@validate_model
def loadfrets(model, start_date, stop_date):
    '''
    load axioma factor returns data over a given window
    
    Parameters:
    model : str
        name of axioma risk model
    start_date : datetime
        start date of factor returns
    stop_date : datetime
        stop date of factor returns

    returns DataFrame
    '''
     
    tmp = pandas.DataFrame()
    for dt in pandas.DateRange(start_date, stop_date):
        _tmp = loadfret(model, dt)
        tmp = tmp.append(_tmp, ignore_index=True)

    return tmp

@validate_model
def loadexp(model, date):
    '''
    load axioma asset exposure data
    
    Parameters:
    model : str
        name of axioma risk model
    date : datetime
        date of axioma risk model

    returns DataFrame
    '''
    return __loader(model, date, 'exp')

@validate_model
def loadrsk(model, date):
    '''
    load axioma asset exposure data
    
    Parameters:
    model : str
        name of axioma risk model
    date : datetime
        date of axioma risk model

    returns DataFrame
    '''
    return __loader(model, date, 'rsk')

@validate_model
def loadcov(model, date):
    '''
    load axioma covariance matrix
    
    Parameters:
    model : str
        name of axioma risk model
    date : datetime
        date of axioma risk model

    returns DataFrame
    '''
    data = __loader(model, date, 'cov')
    return data.set_index('FactorName')

### to hook in to generate_sig_history for historical data    
#def run(date, use_flat=False):
def run(date, use_flat=True):
    if date.weekday() in [5,6]:
        return
   
    for model in VALID_MODELS:
        __generate_ax_files(model, date, use_flatfile=use_flat) 

    if date.day > 30:
        print date


#if __name__ == '__main__':
#    import datetime 
#    usedate = datetime.datetime.today()
#    usedate -= pandas.datetools.BDay()*2
#    run(usedate)

