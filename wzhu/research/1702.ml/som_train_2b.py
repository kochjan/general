#!/opt/anaconda/bin/python
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

MODEL_NAME = 'som_test2b_w5'

'''
strategy 2: use fewer columns to help focus the training

2a: use .T instead of .sum and .std

2b. add past 3 .T

2c. provide a general context about how the signals perform in the past

2d: each signal should use its past history only:  3 .T
'''

# define SIGNALS and BARRA list
SIGNALS = ['b2p_in', 'bpbeta', 'cratio_e', 'e12p_in', 'e2p_in', 'lowsrisk']
BARRA =  ['growth', 'leverage', 'liquidit', 'momentum', 'size', 'value', 'volatili']


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

    # add common CONTEXT columns: turn barra sum/std into count of good or bad styles and signals
    for factor in SIGNALS + BARRA:
        data[factor+'.T'] = data[factor+'.sum']/data[factor+'.std']
        # add prev 3 weeks
        for prev in range(1,4):
            data['prev{}_'.format(prev)+factor+'.T'] = data[factor+'.T'].shift(prev)


    # get ready for training
    # can't use today's knowledge
    ix_today = data['date'] == date
    # for forecast:
    today = data[ix_today]
    # training data
    data = data[-ix_today]

    # select grid size
    N = len(data)
    clumps = float(N)/30
    grid = int(np.sqrt(clumps)/5.0)*5
    if grid < 5:
        grid = 3
    elif grid > 10:
        grid = 10
    print 'N: {}, clumps: {}, grid size chosen: {}'.format(N, clumps, grid)

    # training

    # use factor past week return and month stdev, and perhaps epfr or other macro info
    use_cols = [x for x in data.columns if x.endswith('.T')]

    train_data = data[use_cols]
    train_data = train_data.dropna()

    SOM_X, SOM_Y = grid, grid
    som1 = som.som(SOM_X, SOM_Y, train_data.values, usePCA=False)

    ## input data, number of iterations
    som1.somtrain(train_data.values, TRAIN_STEPS)

    ### model is now trained, walk through each row to get the neighborhood for a given row
    data['hood'] = None
    for cnt, row in train_data.iterrows():
        hood, act = som1.somfwd(row.values)
        data['hood'].ix[cnt] = hood
    ### now take todays data, walk through, and place each row in a neighborhood
    today['hood'] = None
    for cnt, row in today[use_cols].iterrows():
        hood, act = som1.somfwd(row.values)
        today['hood'].ix[cnt] = hood

    predict_cols = [x for x in data.columns if x.endswith('fret')]
    for ret_col in predict_cols:
        hood_ret = data.groupby('hood')[ret_col].median()
        hood_ret = hood_ret.to_dict()

        ### map the return back to securities
        today['pred_'+ret_col] = today['hood'].apply(hood_ret.get)

    today.to_csv(MODEL_NAME+'/forecast_{:%Y%m%d}.csv'.format(date))


if __name__== '__main__':
    from datetime import datetime as dd
    for d in pd.date_range(dd(2012,1,1),dd(2016,12,31)):
        if d.weekday() == 4:
            #run(dd(2010,1,29))
            if COUNTRY_STUDY:
                run(d, only_country=COUNTRY_STUDY)
            else:
                run(d)
