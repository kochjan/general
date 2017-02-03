
import sys
sys.path.insert(0, '/opt/anaconda/lib/')

import nipun.cpa.load_barra as lb
import nipun.utils as nu
from sklearn.utils import shuffle
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
import cPickle

import pandas
import datetime

MPCOLS = 'month_t0,month_t1,month_t10,month_t11,month_t12,month_t2,month_t3,month_t4,month_t5,month_t6,month_t7,month_t8,month_t9'.split(',')
DPCOLS = 'day_t0,day_t1,day_t10,day_t11,day_t12,day_t13,day_t14,day_t15,day_t16,day_t17,day_t18,day_t19,day_t2,day_t3,day_t4,day_t5,day_t6,day_t7,day_t8,day_t9'.split(',')



def train(start, stop):

    data = pandas.DataFrame()
    ### load data and turn into binary classification
    for dt in pandas.date_range(start, stop, freq='M'):
        try:
            tmp = pandas.read_csv('pmom/%s.csv' % dt.to_pydatetime().strftime('%Y%m%d'))
        except:
            continue

        tmp['tclass'] = (tmp['FRET_F1'] > tmp['FRET_F1'].median()).astype(int)
        data = data.append(tmp, ignore_index=True)
  
    ### randomly shuffle data
    data = shuffle(data) 
    data = data.dropna()
    print 'data size'
    print len(data)

    ### if you want to drop a country (jpn?) quickly do it here
#    data['cnt'] = data['BARRID'].apply(lambda x: x.upper()[:3])
#    data.pop('cnt')

    ### parameters
    '''
    params = dict(n_estimators=100, \   #200
                max_depth=2, \          #3
                subsample=0.7, \        #0.7
                random_state=112984, \
                max_features='sqrt', \
                min_samples_split=100, \
                min_samples_leaf=40, \
                learning_rate=0.05)     #0.05
    '''
    params = dict(n_estimators=50, \
                max_depth=2, \
                subsample=0.7, \
                random_state=112984, \
                max_features='sqrt', \
                min_samples_split=100, \
                min_samples_leaf=40, \
                learning_rate=0.05)

    ### build just using monthly data
    print 'building 1m'
    gbrm = GradientBoostingClassifier(**params)
    gbrm.fit(data[MPCOLS], data['tclass'])


    ### build just using daily data
    print 'building daily'
    gbrd = GradientBoostingClassifier(**params)
    gbrd.fit(data[DPCOLS], data['tclass'])

    ### build using both daily and monthly data
    print 'building combo'
    gbrc = GradientBoostingClassifier(**params)
    gbrc.fit(data[MPCOLS+DPCOLS], data['tclass'])

    ### save the resulting models in a pickle file that we can recall later to test
    fh = file('pmom_models/combo_wtd.mdl', 'wb')
    cPickle.dump({'gbrm': gbrm, 'gbrd':gbrd, 'gbrc':gbrc}, fh)
    fh.close()

    return 


def run_me(date, models):

    ret = lb.loadret('ase1jpn', 'S', date)
    ret['rev'] = ret['RET'] * -1
    nu.write_alpha_files(ret['rev'], 'reversal_1m', date)
    
    data = pandas.read_csv('pmom/%s.csv' % date.strftime('%Y%m%d'), index_col=0)
    data = data.dropna()


    for model in models:
        data['gbd'] = models['gbrd'].predict_proba(data[DPCOLS])[:,1]
        data['gbm'] = models['gbrm'].predict_proba(data[MPCOLS])[:,1]
        data['gbc'] = models['gbrc'].predict_proba(data[MPCOLS+DPCOLS])[:,1]

    print 'correlation:'
    print data[['FRET_F1', 'FRET_F2', 'FRET_F3', 'gbd', 'gbm', 'gbc']].corr()
    # export ALPHADIR=/home/wzhu/gitme/general/david/ml/pmom_models/
    nu.write_alpha_files(data['gbd'], 'ml_pmom_monthly_gbd', date)
    nu.write_alpha_files(data['gbm'], 'ml_pmom_monthly_gbm', date)
    nu.write_alpha_files(data['gbc'], 'ml_pmom_monthly_gbc', date)
    return data


def run(date):
        
    fh = file('pmom_models/combo_wtd.mdl', 'rb')
    models = cPickle.load(fh)
    run_me(date, models)

'''
if __name__=='__main__':
    # run with:  apy pmom.py
    train(datetime.datetime(2015,1,31), datetime.datetime(2015,7,31))
'''
