#!/home/wzhu/anaconda/bin/python
import numpy as np
import pandas as pd
import datetime
import pickle
import som
import os

COUNTRY_STUDY = 'TWN'
DEBUG = False #True
TRAIN_STEPS = 10 if DEBUG else 50

BENCHMARK = False
WINDOW_SIZE = 5 #week

MODEL_NAME = 'som_test1_w5'

if COUNTRY_STUDY:
    MODEL_NAME += '_'+ COUNTRY_STUDY
if BENCHMARK:
    MODEL_NAME += '_bm'

# create path
if not os.path.exists(MODEL_NAME):
    os.makedirs(MODEL_NAME)

MAX_LOOKBACK_MONTHS = 0 #60

#SOM_X, SOM_Y = 10, 10 # set dynamically below

#combine all data into single dataframe with date/country as index
with open('country_fret_panel.{}'.format(WINDOW_SIZE),'rb') as infh:
    allpanel=pickle.load(infh)

dataDF = None
for cty in allpanel.items:
    df = allpanel.get(cty)
    df['COUNTRY'] = cty
    df = df.reset_index().rename(columns={'index':'date'})
    df['weekday'] = df['date'].apply(lambda x: x.weekday())

    # for weekly study, only use Friday data to forecast next week
    df = df[ df['weekday']==4 ]

    if dataDF is None:
        dataDF = df
    else:
        dataDF = pd.concat([dataDF, df])

def run(date, only_country=None):
    '''
    '''
    print date

    data = dataDF[dataDF['date'] <= date]

    if MAX_LOOKBACK_MONTHS>0:
        min_ret_date = date - pandas.datetools.MonthEnd() * MAX_LOOKBACK_MONTHS
        data = data[data['date'] >= min_ret_date]

    if only_country:
        data = data[data['COUNTRY']==only_country]

    # whether we want to contrast by creating a benchmark that doesnt use priviledge info
    if BENCHMARK:
        del data[EPFR]

    # can't use today's knowledge
    ix_today = data['date'] == date
    today = data[ix_today]
    data = data[-ix_today]

    # use factor past week return and month stdev, and perhaps epfr or other macro info
    use_cols = [x for x in data.columns if x.endswith('.sum') or x.endswith('.std')]
    predict_cols = [x for x in data.columns if x.endswith('fret')]

    train_data = data[use_cols]
    N = len(train_data)
    clumps = float(N)/30
    grid = int(np.sqrt(clumps)/5.0)*5
    if grid < 5:
        grid = 3
    elif grid > 10:
        grid = 10

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
    for cnt, row in today[use_cols].iterrows():
        hood, act = som1.somfwd(row.values)
        today['hood'].ix[cnt] = hood


    if DEBUG: print 'group'

    for ret_col in predict_cols:
        hood_ret = data.groupby('hood')[ret_col].median()
        hood_ret = hood_ret.to_dict()

        ### map the return back to securities
        today['pred_'+ret_col] = today['hood'].apply(hood_ret.get)

    today.to_csv(MODEL_NAME+'/forecast_{:%Y%m%d}.csv'.format(date))

    if DEBUG: print 'done'

if __name__== '__main__':
    from datetime import datetime as dd
    for d in pd.date_range(dd(2012,1,1),dd(2016,12,31)):
        if d.weekday() == 4:
            #run(dd(2010,1,29))
            if COUNTRY_STUDY:
                run(d, only_country=COUNTRY_STUDY)
            else:
                run(d)
