import nipun.cpa.load_barra as lb
import pandas


def run1(date):

    RSKDATA = lb.loadrsk2("ASE1JPN","S", date, daily=True)
    npxchnpak = lb.load_production_universe('npxchnpak', date)
   
    data = pandas.merge(RSKDATA[['COUNTRY', 'LOC_CAPT']], npxchnpak, left_index=True, right_index=True)
 
    data = data[data['COUNTRY']=='KOR']
    data['VALID_UNTIL'] = data['VALID_UNTIL'].map(lambda x:x.strftime('%Y%m%d'))
    data['rank'] = data['LOC_CAPT'].rank()
    data = data.sort('rank')
   
    nbot = len(data) / 2
    ntop = len(data) - nbot
     
    bot = data.head(nbot)
    top = data.tail(ntop)
    
    print bot.head()
    print top.head()

    
    bot = bot[['SEDOL','NATURAL','VALID_UNTIL']]
    top = top[['SEDOL','NATURAL','VALID_UNTIL']]

    bot.to_csv("/research/data/universe/kor_small/kor_small"+date.strftime('%Y%m%d')+".univ",header=False,sep='|')
    top.to_csv("/research/data/universe/kor_big/kor_big"+date.strftime('%Y%m%d')+".univ",header=False,sep='|')


def run(as_of_date):

    """ load Barra risk data """

    RSKDATA = lb.loadrsk2("ASE1JPN","S",as_of_date, daily=True)
    npxchnpak = lb.load_production_universe('npxchnpak',as_of_date)
    #topbot = pandas.read_csv("/research/data/prealpha/topbot_npxchnpak/topbot_npxchnpak_"+as_of_date.strftime("%Y%m%d")+".alp",header=False, \
    #         names=['BARRID','TOPBOT'])

    # old research version of the files

    nextmonth = as_of_date + datetime.timedelta(1)
    print nextmonth, as_of_date

    try:
        filename = "/production/%s/%s/%s/prealpha/ic_scaling_npxchnpak_%s.alp" % (nextmonth.strftime('%Y'), nextmonth.strftime('%m'), nextmonth.strftime('%Y%m%d'), \
                                as_of_date.strftime('%Y%m%d'))
        topbot = (pandas.read_csv(filename,index_col=0)).rename(columns={'ic1':'BIG'})
    except:
        print 'rolling back!'
        topbot = pandas.read_csv("/research/data/prealpha/icscale_npxchnpak/icscale_npxchnpak_"+as_of_date.strftime("%Y%m%d")+".alp",header=False, \
             names=['BARRID','BIG'])
        
#        topbot = pandas.read_csv(topbot,header=True, names=['BARRID','BIG'])

    topbot = topbot.reset_index()
    univdata = npxchnpak.join(RSKDATA[['COUNTRY', 'USD_CAPT']],how='left')
    univdata = univdata[univdata['COUNTRY']=='JPN']
    univdata = topbot.join(univdata,on='BARRID',how='right')
    univdata.index = univdata.pop('BARRID')
    univdata['VALID_UNTIL'] = univdata['VALID_UNTIL'].map(lambda x:x.strftime('%Y%m%d'))

    #univdata_top = univdata[univdata['BIG']=='JPN_BIG']
    univdata_top = univdata[univdata['BIG']<1]
    univdata_top = univdata_top[['SEDOL','NATURAL','VALID_UNTIL']]

    univdata_bot = univdata[(univdata['BIG']=='JPN') | (univdata['BIG'] == 1)]
#    univdata_bot = univdata[univdata['BIG']==1]
    univdata_bot['rnk'] = univdata_bot['USD_CAPT'].rank()
    univdata_bot = univdata_bot.sort('rnk')


    print univdata_bot.head().to_string()
    univdata_bot = univdata_bot[['SEDOL','NATURAL','VALID_UNTIL']]
    univdata_bot.to_csv('/research/data/universe/jpnx400/jpnx400'+as_of_date.strftime('%Y%m%d')+'.univ', header=False, sep='|')
    univdata_bot.tail(600).to_csv('/research/data/universe/jpnx400_t600/jpnx400_t600'+as_of_date.strftime('%Y%m%d')+'.univ', header=False, sep='|')


    nbot = len(univdata_bot) / 2
    ntop = len(univdata_bot) - nbot
    print univdata_bot.head().to_string()
    print univdata_bot.tail().to_string()
    

    univdata_bot.head(nbot).to_csv("/research/data/universe/jpnx400_small/jpnx400_small"+as_of_date.strftime('%Y%m%d')+".univ",header=False,sep='|')
    univdata_bot.tail(ntop).to_csv("/research/data/universe/jpnx400_big/jpnx400_big"+as_of_date.strftime('%Y%m%d')+".univ",header=False,sep='|')

    #UNIVDATA.to_csv("/research/data/universe/barraestu/barraestu.univ"+yymm,header=False)
#    univdata_top.to_csv("/research/data/universe/jpn400/jpn400"+as_of_date.strftime('%Y%m%d')+".univ",header=False,sep='|')

    return True

import datetime


