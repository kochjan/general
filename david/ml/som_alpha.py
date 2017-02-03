import nipun.utils as nu
import pandas
import datetime
import nipun.cpa.load_barra as lb

import sys
sys.path.insert(0,'/home/wzhu/gitme/general/david/ml/')
import som

DEBUG = True
BENCHMARK = True

MODEL_NAME = 'som_ir_ncty_20'
if BENCHMARK:
    MODEL_NAME += '_bm'
EPFR = 'epfr'

MAKE_COUNTRY_PIVOT = False
MAX_CLUSTER_LENGTH = 12
MAX_IMPULSE_LENGTH = 3

RETURN_COL = 'resid_c'
SOM_X, SOM_Y = 5, 5

def make_country(data):
    '''
    Input:
    data has default columns 'BARRID', 'date', 'COUNTRY', RET
    data extra columns: sig1, sig2, ..., sigN

    Output:
    columns: BARRID, date, sig1_CTYa, ig1_CTYb, ..., sig2_CTYa, sig2_CTYb, ..., sigN_CTY_C,   RET
    '''

    ret_data = data[['BARRID', 'date', RETURN_COL]].copy()
    
    data = data.set_index(['BARRID', 'date', 'COUNTRY'])
    data.pop(RETURN_COL)
    data = data.stack()
    # now sig1,sig2,..., sigN info goes to N rows: column_name, value
    data = data.reset_index()
    data['new_col'] = data['level_3'] +'_'+ data['COUNTRY']
    data = pandas.pivot_table(data, index=['BARRID', 'date'], columns='new_col', values=0)
    # index = [BARRID, date], columns = [sig1_CTYa, sig1_CTYb, ..., sig2_CTYa, sig2_CTYb, ..., sigN_CTY_C]
    # for the NxC columns, there are (N-1)xC NaNs for the N-1 countries that are not for that row of data
    data = data.reset_index()
    data = pandas.merge(data, ret_data, on=['BARRID', 'date'])
    data = data.fillna(0.0)
#    data.pop('iu_JPN')

    return data


def run(date):
    '''
    How many lead days (data warmup) do we need to allot?  For now, leave it at one year
    '''
    print date
    data = pandas.DataFrame()
    dts = pandas.DateRange(date-pandas.datetools.MonthEnd()*MAX_CLUSTER_LENGTH, date, offset=pandas.datetools.MonthEnd())
    min_ret_date = date - pandas.datetools.MonthEnd() * MAX_IMPULSE_LENGTH

    for dt in dts:
        try:
            _tmp = pandas.read_csv('som/som.input_data/%s.csv' % dt.strftime('%Y%m%d'))
            data = data.append(_tmp, ignore_index=True)
        except:
            pass
            print 'no data for %s' % dt

    ix = data['COUNTRY'] != 'IND'
    data = data[ix]


    if MAKE_COUNTRY_PIVOT:
        data = make_country(data)
    else:
        del data['COUNTRY']

    # whether we want to contrast by creating a benchmark that doesnt use priviledge info
    if BENCHMARK:
        del data[EPFR]
    # can't use today's knowledge
    ix_today = data['date'] == int(date.strftime('%Y%m%d'))
    today = data[ix_today]
    data = data[-ix_today]

    use_cols = data.columns.tolist()
    use_cols.remove('resid_c')
    use_cols.remove('BARRID')
    use_cols.remove('date')
    
    som1 = som.som(SOM_X, SOM_Y, data[use_cols].values, usePCA=False)

    '''
    if DEBUG:
        print 'train'
        data[use_cols].to_csv('som/som.train/%s.csv' % date.strftime('%Y%m%d'))
        return
    '''

    ## input data, number of iterations
    som1.somtrain(data[use_cols].values, 20)

    if DEBUG: print 'hood'
    ### model is now trained, walk through each row to get the neighborhood for a given row
    data['res'] = None
    for cnt, row in data[use_cols].iterrows():
        hood, act = som1.somfwd(row.values)
        data['res'].ix[cnt] = hood

    if DEBUG: print 'group'
    ### group each neighborhood to get the forward return
    ix = data['date'] >= int(min_ret_date.strftime('%Y%m%d'))
    hood_ret = data[ix].groupby('res')[RETURN_COL].median()
    hood_ret = hood_ret.to_dict()

    if DEBUG: print 'today'
    ### now take todays data, walk through, and place each row in a neighborhood
    today['res'] = None
    for cnt, row in today[use_cols].iterrows():
        hood, act = som1.somfwd(row.values)
        today['res'].ix[cnt] = hood

    ### map the return back to securities
    today['pret'] = today['res'].apply(hood_ret.get)
    today = today[['BARRID', 'pret']]
    today.set_index('BARRID', inplace=True)

    nu.write_alpha_files(today, MODEL_NAME, date)
    if DEBUG: print 'done'
