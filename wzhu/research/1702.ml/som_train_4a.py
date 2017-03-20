#!/opt/anaconda/bin/python
import numpy as np
import pandas as pd
import datetime
import pickle
import som
import os

# emulate David: 6 months alpha

COUNTRY_STUDY = 'TWN'
MODEL_NAME = 'som_stock4a_'+ COUNTRY_STUDY

DEBUG = False
CLUSTERINFO=True
TRAIN_STEPS = 5 if DEBUG else 50
GRIDMIN = 3
GRIDMAX = GRIDMIN if DEBUG else 10


# create path
if not os.path.exists(MODEL_NAME):
    os.makedirs(MODEL_NAME)

MAX_LOOKBACK_MONTHS = 6 #60

#SOM_X, SOM_Y = 10, 10 # set dynamically below

#combine all data into single dataframe with date/country as index
with open('bf.twn.expectAlpha_v2.monthly.train','rb') as infh:
    dataDF=pickle.load(infh).reset_index()

def run(date):
    '''
    '''
    print date


    data = dataDF[dataDF['date'] <= date]
    data = data.dropna()

    if MAX_LOOKBACK_MONTHS>0:
        min_ret_date = date - pd.tseries.offsets.MonthEnd() * MAX_LOOKBACK_MONTHS
        data = data[data['date'] >= min_ret_date]

    #if only_country:
    #    data = data[data['COUNTRY']==only_country]

    # decide data columns, then standardize:
    # use factor past week return and month stdev, and perhaps epfr or other macro info
    use_cols = [x for x in data.columns if x.endswith('TWN')]
    predict_cols = ['NextRET',]

    for col in use_cols:
        data[col] = (data[col] - data[col].mean())/data[col].std()
        winsor_idx = data[col] > 3
        data.ix[winsor_idx, col] = 3
        winsor_idx = data[col] < -3
        data.ix[winsor_idx, col] = -3



    # can't use today's knowledge
    ix_today = data['date'] == date
    today = data[ix_today]
    data = data[-ix_today]

    train_data = data[use_cols]
    N = len(train_data)
    clumps = float(N)/30

    grid = int(np.sqrt(clumps))
    if grid < GRIDMIN:
        grid = GRIDMIN
    elif grid > GRIDMAX:
        grid = GRIDMAX

    print 'N: {}, clumps: {}, grid size chosen: {}'.format(N, clumps, grid)
    SOM_X, SOM_Y = grid, grid

    if DEBUG:
        print 'train'
        print train_data.head(2).T

    som1 = som.som(SOM_X, SOM_Y, train_data.values, usePCA=False)

    ## input data, number of iterations
    som1.somtrain(train_data.values, TRAIN_STEPS)

    if DEBUG: print 'hood'
    ### model is now trained, walk through each row to get the neighborhood for a given row
    data['hood'] = None
    for cnt, row in data[use_cols].iterrows():
        hood, act = som1.somfwd(row.values)
        data['hood'].ix[cnt] = hood
    ### now take todays data, walk through, and place each row in a neighborhood
    today['hood'] = None

    if not CLUSTERINFO:
        for cnt, row in today[use_cols].iterrows():
            hood, act = som1.somfwd(row.values)
            today['hood'].ix[cnt] = hood
    else:
        today['hood_ct'] = None
        today['hood_z'] = None
        today['hood_avg'] = None
        today['hood_std'] = None
        for cnt, row in today[use_cols].iterrows():
            hood, act, csize, czscore, cavg, cstd = som1.somfwd(row.values, clusterSizeFlag=True)
            today['hood'].ix[cnt] = hood
            today['hood_ct'].ix[cnt] = csize
            today['hood_z'].ix[cnt] = czscore
            today['hood_avg'].ix[cnt] = cavg
            today['hood_std'].ix[cnt] = cstd


    if DEBUG: print 'group'

    for ret_col in predict_cols:
        hood_ret = data.groupby('hood')[ret_col].median()
        hood_ret = hood_ret.to_dict()

        ### map the return back to securities
        today['pred_'+ret_col] = today['hood'].apply(hood_ret.get)

    today.to_csv(MODEL_NAME+'/forecast_{:%Y%m%d}.csv'.format(date))

    if DEBUG: print 'done'

if __name__ == '__main__':
    from datetime import datetime as dd
    if DEBUG:
        run(dd(2010, 1, 31))
    else:
        for d in pd.date_range(dd(2008,1,1),dd(2011,12,31), freq='M'):
            run(d)
