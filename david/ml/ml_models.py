import time
import sys
sys.path.insert(0, '/opt/anaconda/lib/')
sys.path.insert(1, '/home/dsargent/github/research/ml/')
import pandas
import numpy
import datetime

from sklearn.cross_validation import StratifiedKFold, KFold
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import *
from sklearn.neighbors import KNeighborsClassifier
from sklearn.linear_model import LinearRegression, SGDClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.utils import shuffle
from sklearn.preprocessing import RobustScaler

import matplotlib.pyplot as plt
import nipun.utils as nu
import nipun.cpa.winsor as wins
import nipun.cloud.data as cd
import nipun.cpa.load_barra as lb
from scipy.stats import rankdata
import cPickle as cpickle

import nipun.trading_days as td

VERSION = 'ml-base-v2'

X_COLS = [
 'volatility',
 'resid_c_5', 
# 'resid_c_1', 
# 'resid_c_3', 
 'resid_c_10',
 'resid_c_20',
 'b2proe',
 'roa_fast',
 'fcf_oa_fast',
 'updn2midc',
 'stvol',
 'stliq',
 'd2p',
 'e2p',
 'pmom',
 'price_target',
 'e12p_tsv',
 'USD_CAPT'
 ]

X_COLS = map(lambda x: x.replace('resid_c', 'RET'), X_COLS)


Y_COLS = 'TARGET'
TARGET_COL = 'FWD_RET_20'
X_COLS_O = X_COLS[:]

### this is helpful so that you get consistent runs from one test to the next!
RANDOM_STATE = 1129


def prep_data(data, lag=2):
    """standardize and prep data and target variables- compute any lags..."""

    global X_COLS
    LAG_WINDOW = lag
    LAG_COLS = []
    data.loc[:,'TARGET'] = 0.0
    data = data[data[TARGET_COL].notnull()]

    def lagger(tmp):
        tmp[LAG_COLS] = tmp[X_COLS].diff(LAG_WINDOW)
        return tmp

    for col in X_COLS:
        ix = data[col].isnull()
        data.loc[ix,col] = 0.0
        lag_c = '%s_lag%s' % (col, LAG_WINDOW)
        data.loc[:,lag_c] = pandas.np.nan
        LAG_COLS.append('%s_lag%s' % (col, LAG_WINDOW))

    data = data.sort('datadate')
    data = data.groupby('barrid').apply(lagger)
    X_COLS = X_COLS + LAG_COLS

    for country in data['COUNTRY'].unique():
        ix = (data['COUNTRY'] == country) #& (data['datadate'] == datadate)
        for col in X_COLS:
            try:
                data.loc[ix, col] = wins.winsor(data.loc[ix, col])
            except Exception, e:
                data.loc[ix, col] = 0.0

    udts = sorted(data['datadate'].unique())[LAG_WINDOW:]
    data = data[data['datadate'].isin(udts)]
    data = data.fillna(0)
    return data


def balance_classifier(X, Y, upsize=False):
    """Build a balanced classifier - in the event you're trying to predict the top 10% you may want to try balancing"""

    n_pos = Y.sum() 
    ix_neg = pandas.np.where(Y == 0)[0]

    ### if we need more positive instances!
    if upsize:
        ### create N copies of positive instances so N positive ~= N negative
        ### if input vector is 100 , with 10 positive, 90 negative rows
        ### the resulting output will be 10 positive instances (9 times) so 90, plus 90 negative
        n_pos_needed = (len(ix_neg) // n_pos)
        ix_pos = pandas.np.where(Y == 1)[0] 
        x_pos = pandas.np.vstack([X[ix_pos]]*n_pos_needed)
        y_pos = pandas.np.hstack([Y[ix_pos]]*n_pos_needed)
        
        X = pandas.np.vstack((x_pos, X[ix_neg]))
        Y = pandas.np.hstack((y_pos, Y[ix_neg]))

    else:
        ### find the number of positive instances in data set
        ### pull out a random sampling of the same number of negative instances
       

        rng = pandas.np.random.RandomState(RANDOM_STATE)
        rng.shuffle(ix_neg)
          
        ix_neg = ix_neg[:n_pos]
        ix_pos = pandas.np.where(Y == 1)[0]
    
        ix_test = pandas.np.hstack((ix_neg, ix_pos))
        X = X[ix_test]
        Y = Y[ix_test]


    X, Y = shuffle(X, Y)
    return X, Y
    

### The follwing *_test functions are purely helpers for quickly/interactively running consistent tests
### if you want to evaulate how a given model performs in a consistent manner
### these will evolve as you iterate and what you want to look at - but this makes it fairly consistent
### for sayign "ok, lets quickly test random forests predicting Y - if I change the depth parameter what happens"
### there is a helper function "cross_validate" which basically just takes , data: X, Y, and the number of folds, and the model to use
### and it will call the approriate model test.  
### this is helpful because 
#

def gbrc_test(model, folds, X, Y):

    tree_stat = pandas.DataFrame()
    iter_no = 0

    kf = KFold(len(Y), n_folds=folds)
    iter_no = 0
    for (train, test) in kf: 

        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]

        print 'Initial:', X_train.shape, X_test.shape, Y_train.sum(), len(Y_train)
        X_train, Y_train = balance_classifier(*shuffle(X_train, Y_train), upsize=False) #True)
        print 'Balance:', X_train.shape, X_test.shape, Y_train.sum(), len(Y_train)
        model.fit(X_train, Y_train)
        score = model.score(X_test, Y_test)
        Y_p = model.predict(X_test)
        
        print iter_no, score, model.score(X_train, Y_train)
        print len(Y_test), Y_test.sum()
        print classification_report(Y_test, Y_p)

        iter_no += 1

    return tree_stat
   

def gbr_test(model, folds, X, Y):

    tree_stat = pandas.DataFrame()
    iter_no = 0
    for (train, test) in folds:
        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        X_train, Y_train = shuffle(X_train, Y_train)
        model.fit(X_train, Y_train)

        ### get model performance by test/train at each step
        test_score = pandas.np.zeros((model.n_estimators,), dtype=pandas.np.float64)
        for i, y_pred in enumerate(model.staged_predict(X_test)):
            test_score[i] = pandas.np.corrcoef(Y_test, y_pred)[0,1]
#            test_score[i] = model.loss_(Y_test, y_pred)

        ### get model performance by test/train at each step
        train_score = pandas.np.zeros((model.n_estimators,), dtype=pandas.np.float64)
        for i, y_pred in enumerate(model.staged_predict(X_train)):
            train_score[i] = pandas.np.corrcoef(Y_train, y_pred)[0,1]

        tmp = pandas.DataFrame({'train':train_score,  \
                                'test': test_score, \
                                'tree_no': range(model.n_estimators), 
                                'iter': [iter_no]*model.n_estimators})
        tree_stat = tree_stat.append(tmp, ignore_index=True)

        score = model.score(X_test, Y_test)
        Y_p = model.predict(X_test)
        print iter_no, score, model.score(X_train, Y_train), pandas.np.corrcoef(Y_test, Y_p)[0,1]
        iter_no += 1
    
    return tree_stat

def rdf_test(model, folds, X, Y):
    
    tree_stat = pandas.DataFrame()
    iter_no = 0
    for (train, test) in folds:
        
        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        model.fit(X_train, Y_train)

        Y_p = model.predict(X_test)
        score = model.score(X_test, Y_test)
        print iter_no, model.score(X_train, Y_train), score, pandas.np.corrcoef(Y_test, Y_p)[0,1]
        iter_no += 1        


def knnc_test(model, folds, X, Y):
    
    tree_stat = pandas.DataFrame()
    iter_no = 0
    for (train, test) in folds:
        
        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        model.fit(X_train, Y_train)
        yp = model.predict(X_test)

        print classification_report(Y_test, yp)
        cm = confusion_matrix(Y_test, yp)
        cm_n = cm.astype('float') / cm.sum(axis=1)[:, pandas.np.newaxis]
        print cm_n
    return

def linear_test(model, folds, X, Y):
    
    iter_no= 0
    for (train, test) in folds:

        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        model.fit(X_train, Y_train)
        Y_p = model.predict(X_test)
        print iter_no, model.score(X_train, Y_train), model.score(X_test, Y_test), pandas.np.corrcoef(Y_test, Y_p)[0,1]
        iter_no +=1


def cross_validate(X, Y, nfolds, mx):

    kf = KFold(len(Y), n_folds=nfolds)
    error_stat = []

    if isinstance(mx, GradientBoostingRegressor):
        return gbr_test(mx, kf, X, Y)

    elif isinstance(mx, (KNeighborsClassifier, RandomForestClassifier, GradientBoostingClassifier)):
        return knnc_test(mx, kf, X, Y)

    elif isinstance(mx, GradientBoostingClassifier):
        return gbrc_test(mx, nfolds, X, Y)

    elif isinstance(mx, LinearRegression):
        return linear_test(mx, kf, X, Y)

    elif isinstance(mx, (ExtraTreesRegressor, RandomForestRegressor)):
        return rdf_test(mx, kf, X, Y)

    i = 0
    for (train, test,) in kf:
        print 'iteration no:', i

        (X_train, X_test,) = (X[train], X[test])
        (Y_train, Y_test,) = (Y[train], Y[test])
        mx.fit(X_train, Y_train)
        score = mx.score(X_test, Y_test)
        print score
        error_stat.append({'score': score})
        i += 1

        continue
        t_error = []
        for tpredict in mx.staged_predict(X_test):
            t_error.append(1.0 - accuracy_score(tpredict, Y_test))

        n_trees = len(mx)
        score = mx.score(X_test, Y_test)
        error_stat.append({
            'ntrees': n_trees,
            'error': t_error,
            'score': score})
        i += 1

    return error_stat



from sklearn.cluster import *
import warnings
warnings.filterwarnings('ignore')


### build models based on multi-trained composite
def cluster(X, n_folds, params):
    """Test of clustering, not super successfull"""
    kf = KFold(len(X), n_folds=n_folds)
    i = 0
    models = {}

    for (train, test) in kf:
        X_train, X_test = X.ix[train], X.ix[test]

        mb = MiniBatchKMeans(**params)
        mb.fit(X_train[X_COLS].fillna(0))
        X_train['class'] = mb.predict(X_train[X_COLS].fillna(0))
        TCOL = 'FWD_resid_c_10'

        grp = X_train.groupby('class')[TCOL]
        class_ret = pandas.DataFrame({'avg':grp.mean(), 'std': grp.std()})
        class_ret['xgrp_std'] = X_train[TCOL].std()
        class_ret = class_ret.rename(columns={TCOL:'class_fret'})
 
        models['kmean_cl%s' % i] = mb
        models['class_ret%s' % i] = class_ret

        i += 1

    return models


def boost(X, Y, nfolds, params):
    """Build a Gradient Boosting Regressor model using nfolds, of params *params
    
    Return each model trained
    """
    
    kf = KFold(len(Y), n_folds=nfolds)
    i = 0
    models = {}

    for (train, test) in kf:
    
        model = GradientBoostingRegressor(**params)
    
        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        X_train, Y_train = shuffle(X_train, Y_train)
        model.fit(X_train, Y_train)

        models['gbr_10%s' % i] = model
        i += 1

    return models
   
 
def classify(X, Y, nfolds, params, debug=False, upsize=False):
    """Build a gradient boosting classifieri using nfolds and params=params"""

    kf = KFold(len(Y), n_folds=nfolds)
    i = 0
    models = {}

    predict = pandas.DataFrame()
    for (train, test) in kf:
    
        model = GradientBoostingClassifier(**params)
        X_train, X_test = X[train], X[test]
        Y_train, Y_test = Y[train], Y[test]
        X_train, Y_train = shuffle(X_train, Y_train)

        ### be sure to balance classifier!
        Xc, Yc = balance_classifier(X_train, Y_train, upsize=upsize)
        model.fit(Xc, Yc)
        models['mm_%s' % i] = model
 
        if debug:
            y_debug = model.predict(X[test]) 
            predict['v%si' % i] = model.predict_proba(X_test)[:,1]
        i += 1

    return models
    
def run(date, run_model=True, itersize=7):
    """helper if you want to run weekly/daily etc"""

    pmonth = date - pandas.datetools.MonthEnd()
    data = load_data(date)
    run_me(date, data.copy())
    return

    while date > pmonth:
        run_me(date, data.copy())
        date -= datetime.timedelta(itersize)

    return



def load_data(date, window=400):

    dg = lambda x: date - pandas.datetools.BDay()*x
    udate = date

    dates = [dg(x) for x in range(1, window, 10)]
    if not date.weekday() in [5,6]:
        udate = date - pandas.datetools.BDay()

    data = pandas.DataFrame()
    for dt in dates:
        tmp = cd.read_gce('users', 'dsargent/{v}/{d}.pd'.format(v=VERSION, d=dt.strftime('%Y%m%d')))
        if tmp is None:
            print 'no data: ', dt
            continue

        tmp = tmp.reset_index().rename(columns={'index': 'barrid'})
        data = data.append(tmp, ignore_index=True)
    return data
 

def run_me(date, data, run_model=True, data_only=False):

    data = data[(data['USD_CAPT'] > 1e9) & (data['COUNTRY'] != 'CHN')]
    max_ret_days = td.get_trading_dates(date, 12)
    max_ret_date = date - datetime.timedelta(max_ret_days)
    data = data[(data['datadate'] < max_ret_date)]

    global X_COLS, X_COLS_O
    X_COLS = X_COLS_O[:]
    model = prep_data(data, lag=4)

    print date, data['datadate'].max(), max_ret_date
    X, Y5, Y5N, Y10 = shuffle(model, model['FWD_resid_c_5'], model['fwd_n5d_resid'], model['FWD_resid_c_10'])

    base_params = dict(n_estimators=250,
                        max_depth=2,
                        max_features='sqrt',
                        learning_rate=0.05,
                        loss='lad', 
                        min_samples_leaf=50,
                        subsample=0.9)

    boosting = boost(X[X_COLS].values, Y10.values, 5, base_params)
 
    class_params = dict(n_estimators=200, 
                        max_depth=3,
                        max_features='sqrt', 
                        learning_rate=0.03, 
                        subsample=0.9)

    print 'classify' 
    ix = Y5.notnull()
    
    drawdown5 = classify(X.loc[ix, X_COLS].fillna(0).values, (Y5[ix] < Y5[ix].quantile(0.1)).astype(int).values, 5, class_params)

    fh = file('/tmp/%s.mdl' % date.strftime('%Y%m%d'), 'w')
    cpickle.dump({'dd5': drawdown5, 
                'boosting': boosting}, fh)
    fh.close()
    return
   
