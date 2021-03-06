#!/opt/python2.7/bin/python
'''
script to run weekly/monthly that will copy over production daily or month end alpha files
in the event the alpha file has not changed, otherwise it will requeue
and regenerate a long daily/monthly history of the alpha file

options:
   univ: list of universes
   alphas: list of alpha code file names
   freq: daily or monthly
   start_date: the earliest date for regenerating alphas
   n: the max number of alphas to be regenerated
   symlk: if True, then make symlink, o.w. make hard copy
   
e.g.
1. python update_alpha.py --univ='npxchnpak, jpn400' --freq='daily' --start_date='20150101' --alphas='prod_sales_surprise.py, prod_sss.py, prod_abr.py'
   It forces to recalculate daily .alp files for prod_sales_surprise.py, prod_sss.py, prod_abr.py since 2015-01-01.
   
2. python update_alpha.py --univ='npxchnpak, jpn400' --freq='monthly' --start_date='20150101' --n=1
   It is to hard copy all .alp files for 2 universes, only for the latest monthend. if some alpha code file is updated between the latest month end date and the second latest monthend, then recalculate n=1 of such alphas for every monthend since 2015-01-01. if n=None, we recalculate all updated alphas.

3. python update_alpha.py --univ='npxchnpak, jpn400' --freq='daily' --start_date='20150101'
   It is to hard copy all .alp files for 2 universes, for every day since 2015-01-01. if some alpha code file is updated in the past 7 days, then recalculate n=1 of such alphas. if n=None, we recalculate all updated alphas.

4. for testing ongoing run: python update_alpha.py --univ='npxchnpak' --freq='daily' --start_date='20150601'
5. for testing manual run: python update_alpha.py --univ='npxchnpak' --freq='daily' --start_date='20150601' --alphas='xxx,xxx,xxx'

Note: for example, if you run this code on 03/04/2016, it may generate some alphas or bucket alphas for 20160304. pls ignore those files, they are not accurate.
20160304 files should be generated on 03/05/2016.
'''

import os
import pp
import sys
os.environ['ALPHADIR'] = '/research/production_alphas/daily/current/npxchnpak/'
import glob
import shutil
import pandas
import datetime
from optparse import OptionParser

sys.path.insert(0, '/home/dwang/git_new/')# record_alpha is not in nipun_shared yet, so use my local files first
import nipun.alphagen.alpha_combine as ac
import nipun_task.derived.gen_icscaling as gi
import nipun_task.site_config as site_config
import nipun_task.utility.record_alpha as ra
import nipun.dbc as dbc
import re
import nipun.cpa.load_barra as lb

dbo = dbc.db(connect='qai')
NCPUS = 6
ALPHA_PATH = '/home/sil/production/production_signals/'
#ALPHA_PATH = '/home/dwang/work/alpha_files/code/production_signals/'
DIR = "/research/production_alphas/"

def determine_alphas(univ, the_date, prev_date):
    '''
    for a given universe, find the set of alphas that have changed
    between prev_date and the_date

    Parameters
    ----------
    univ : str
        the universe to check
    the_date: datetime
        the end date
    prev_date: datetime
        the beginning date
        we compare the hash codes of each alpha code file on the_date and prev_date

    returns list-like object

    '''
    
    ### check the alphas
    alp_deltas = ra.alert_alphas(univ, base_date=the_date, prior_date=prev_date)
    alp_deltas = alp_deltas[alp_deltas['file_hash_n'] != alp_deltas['file_hash_o']]

    ### return
    if len(alp_deltas) == 0:
        return []

    else:
        return alp_deltas[['alpha_file', 'universe_n']]


### this will be the parallelized function, it will be slow for some, 
### but changes really wont take place that often, plus it will run lazily
def run_alpha(alpha, universe, alpha_path, alpha_dir, debug_dir, t_delta, stop_date, start_date = datetime.datetime(2005, 1, 1), prod=1):
    '''
    run an alpha file historically to generate monthly/daily alphas of the currently
    implemented version. this will run from 2005 to last month and generate
    monthly/daily alphas for the given alpha file

    Parameters:
    -----------
    alpha : str
        the alpha file name
    universe : str
        the universe to run it over
    alpha_path : str
        the path to the alpha code file
    alpha_dir : str
        the path to store the alpha data file in
    debug_dir : str
        the path to store the debug data file in
    t_delta : pd.datetools.day or pd.datetools.monthEnd
        the time interval
    stop_date : datetime
        the last date
    start_date: datetime
        the first date

    returns None
    '''
    
    from cStringIO import StringIO
    #sys.path.insert(0, '/home/dwang/git_root/')
    import nipun_task.site_config as site_config
    #import imp#;import pdb; pdb.set_trace()
    ### override some parameters
    alpha_dir = '%s/%s/' % (alpha_dir, universe)
    site_config.alpha_dir = alpha_dir
    os.environ['ALPHADIR'] = alpha_dir
    
    site_config.debug_dir = debug_dir

    ### get dates, start -> stop, offset=monthly
    date_queue = pandas.DateRange(start_date, stop_date, offset=t_delta)

    stdout = sys.stdout
    sys.stdout = StringIO()

    ### import and execute
    try:
        mod = imp.load_source(alpha.split('.')[0], '%s/%s/%s' % (alpha_path, universe, alpha))

    except Exception, e:
        print >> sys.stderr, '%s/%s/%s' % (alpha_path, universe, alpha)
        print >> sys.stderr, e
        print >> sys.stderr, 'ERROR: import of %s failed' % alpha
        return None

    ### override universe setting
    mod.UNIVERSE = universe
    if mod.PRODUCTION!=1: return None # if an alpha is nut currently used, then skip it
    mod.PRODUCTION = prod #1
    mod.write_debug_files = lambda *x, **y: None
    if 'nu' in dir(mod): mod.nu.write_debug_files = mod.write_debug_files # monkeypatching, to override write_debug_files function.

    ###########################################
    #this block is to override function write_alpha_files by removing the corresponding .alp file first, if it exists
    #if the existing .alp file is a symlink, this block is very important,
    #o.w. we'll directly overwrite .alp file in production
    #if the existing .alp file is a normal file, this block doesn't hurt
    if 'write_alpha_files' in dir(mod):
        wdf = mod.write_alpha_files
    else:
        wdf = mod.nu.write_alpha_files
    def tmp_f(data, alpha, date, \
                        alpdir=False, dtype=False, \
                        dropnan=True, header=False, production=False):#the production=False is very imptt to prevent overwrite production folder!!!
        
        alpdir = os.getenv("ALPHADIR") + alpha
        pth ='%(alpdir)s/%(alpha)s_%(YYYYMMDD)s.alp' % {'alpdir':alpdir, 'alpha':alpha, 'YYYYMMDD':date.strftime('%Y%m%d')} # the full name of the .alp file that we are going to write
        
        if os.path.exists(pth):
            os.remove(pth)#remove or unlink it
        wdf(data, alpha, date, \
                        alpdir=False, dtype=False, \
                        dropnan=True, header=False, production=False)
    mod.write_alpha_files=tmp_f

    if 'nu' in dir(mod): mod.nu.write_alpha_files=tmp_f
    ##############################################

    ### make sure we have a run func
    if 'run' in dir(mod):
        ### loop 
        for the_date in date_queue:
            ### important: some of these will not have data early on, 
            ### so just fail silently and keep going
            try:
                mod.run(the_date)

            except Exception, e:
                print >> sys.stderr, e
                print >> sys.stderr, 'Error: %s %s' % (alpha, the_date.strftime('%Y%m%d'))

    else:
        ### wtf
        print >> sys.stderr, 'No RUN function defined in %s' % alpha

    return

def requeue_alphas(univ, freq, max_n=None, alphas_i=None):
    '''
    requeue func - given a universe first copy all alpha files
    over and then determine if any alpha files have changed.  those that hvae
    differences from last week/month will be requeue and regenerate the entire daily/monthly
    history of the alpha

    Parameters
    ----------
    univ : str
        the universe of alphas to run
    freq: str
        daily or monthly
    max_n: int
        the max number of alphas that we are going to process
    alphas_i: DataFrame
        contains pairs of univ, alpha that are forced to be regenerated
    returns None
    '''

    global ALPHA_DIR, DEBUG_DIR
    td = datetime.datetime.today()
    ALPHA_DIR = '%(dir)s/%(freq)s/current/'%{'dir':DIR, 'freq':freq} 
    DEBUG_DIR = '%(dir)s/%(freq)s/debug/'%{'dir':DIR, 'freq':freq}
    if freq=='daily':    
        t_delta = pandas.datetools.day
        copy_dates = pandas.DateRange(td - pandas.datetools.week, td - t_delta, offset=pandas.datetools.day) # copy .alp files for all dates in copy_dates
        the_date = td # determine_alphas
        prev_date = the_date - pandas.datetools.week # determine_alphas
        stop_date = td - t_delta if opts.stop_date is None else opts.stop_date #in run_alpha
    elif freq=='monthly':
        #ALPHA_DIR = '/research/production_alphas/monthly/current/' 
        #DEBUG_DIR = '/research/production_alphas/monthly/debug/'
        t_delta = pandas.datetools.monthEnd
        copy_dates = [td-t_delta]#[td +datetime.timedelta(1)- t_delta] the latest monthend, not including td. copy .alp files for all dates in copy_dates
        the_date = td - t_delta
        prev_date = the_date - t_delta
        stop_date = td if opts.stop_date is None else opts.stop_date #in run_alpha
    else:
        raise Exception("freq must be either daily or monthly!")


    if alphas_i is None: #ongoing run
        ### find alphas that have changed
        alphas = determine_alphas(univ, the_date, prev_date)
        if max_n:
            alphas = alphas[:opts.max_n]
    else: #manual run
        alphas = alphas_i

    ### first copy everything
    if not alphas_i:# alphas_i is None, i.e. it is ongoing run.
        
        ### where does it come from
        copy_source = site_config.daily_alpha_dir # '/production//daily_alphas/'
        copy_dest = ALPHA_DIR + '/' + univ
        root_name = lambda x: os.path.basename(os.path.dirname(x))
        
        ### replacements
        fns = []
        for copy_date in copy_dates:
            repls = {'root': copy_source, 'univ': univ, 'date':copy_date.strftime('%Y%m%d')}
            fns = fns + glob.glob('{root}/{univ}/alphas/*/*{date}.alp'.format(**repls))# includes all alphas
        

        ### run through and copy each one
        for alpfile in fns:
            
            dest_dir = '%s/%s' % (copy_dest, root_name(alpfile))
            print copy_dest, dest_dir
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
        
            if opts.symlk:
                symlk_name = os.path.join(dest_dir, os.path.basename(alpfile))
                if os.path.exists(symlk_name):
                    os.unlink(symlk_name)
                os.symlink(alpfile, symlk_name)
            else:
                shutil.copy(alpfile, dest_dir)
 
  
    ### now after copying, if any file has changed, we'll requeue those files...
    ### this one runs 1 day at a time, but parallelizes by alpha... may be a little slow
    ### but we don't expect this to really change that often
    if len(alphas) > 0:

        print alphas
        jobs = []
        pps = pp.Server(ppservers=(), ncpus=NCPUS)        

        for alpha, univ in alphas.values:
            imps = ('pandas', 'datetime', 'sys', 'os', 'imp')
            #import imp;run_alpha(alpha, univ, ALPHA_PATH, ALPHA_DIR, DEBUG_DIR, t_delta, stop_date, opts.start_date)
            jobs.append(pps.submit(run_alpha, (alpha, univ, ALPHA_PATH, ALPHA_DIR, DEBUG_DIR, t_delta, stop_date, opts.start_date, opts.prod), (), imps))
            #run_alpha(alpha, univ, ALPHA_PATH, ALPHA_DIR, DEBUG_DIR, t_delta, stop_date, opts.start_date)
        ### check em
        for job in jobs:
            job()

        pps.destroy()
    ### all done
    return


def backfill_b_alphas(weights_date=None, buckets=['analyst', 'fmom', 'industry', 'iu', 'quality', 'sentiment', 'special', 'value'], univ='npxchnpak', alpha='alpha_v5', startdate=datetime.datetime(2005,1,1), enddate=datetime.datetime.today() - pandas.datetools.day, model = 'ase1jpn', ncpus=8, freq='daily'):
    """
    this function is to calculate bucket alphas based on the latest backfilled
    raw alphas
    """
    print "start to backfill bucket alphas eq_all and eq_c"
    if weights_date is None:
        ctry_wt_df = dbo.query("select * from alphagen..country_wt__%s"%alpha, df=True)
        alpha_wt_df = dbo.query("select * from alphagen..alpha_wt__%s"%alpha, df=True)
    else:
        ctry_wt_df = dbo.query("select * from production_reporting..country_wt_backup \
        where cast(datadate AS date)='%(date)s' and alphagen_vers='%(alpha)s'"%{'alpha':alpha, 'date':weights_date}, df=True)
        alpha_wt_df = dbo.query("select * from production_reporting..alpha_wt_backup \
        where cast(datadate AS date)='%(date)s' and alphagen_vers='%(alpha)s'"%{'alpha':alpha, 'date':weights_date}, df=True)

    ctry_wt_df = ctry_wt_df[['alpha_name', 'country', 'weight']]
    alpha_wt_df = alpha_wt_df[['bucket_name', 'alpha_name']]
    bucket_alpha_df = pandas.merge(alpha_wt_df, ctry_wt_df, on=['alpha_name'], how='left')

    job_server = pp.Server(ncpus)
    jobs = []
    for date in pandas.DateRange(startdate, enddate, offset=pandas.datetools.day):
        ctry_df = pandas.DataFrame(lb.loadrsk2(model, 'S', date, daily=True)['COUNTRY'])
        univ_df = lb.load_production_universe(univ, date)
        for bkt in buckets:
            bkt_df = bucket_alpha_df[bucket_alpha_df['bucket_name']==bkt]#.drop(labels=['bucket_name'], axis=1)
            bkt_df = bkt_df.pivot(index='country', columns='alpha_name', values='weight')
            #backfill_1b(date, bkt, bkt_df, ctry_df=ctry_df, univ_df=univ_df, dir=DIR)
            jobs.append(job_server.submit(backfill_1b, (date, bkt, bkt_df, univ, alpha, freq, model, ctry_df, univ_df, DIR), (), ('pandas', 'datetime', 'os')))


    for job in jobs:
        job()

    job_server.print_stats()
    job_server.destroy()

def backfill_1b(date, bkt, bkt_df, univ='npxchnpak', alpha='alpha_v5', freq='daily', model = 'ase1jpn', ctry_df=None, univ_df=None, dir=None):
    """
    backfill one bucket
    """
    import nipun.cpa.load_barra as lb
    #import pandas.util.py3compat as py3compat
    
    print "backfill bucket %s for %s"%(bkt, date)
    
    date_str = date.strftime('%Y%m%d')
    keep_alphas = bkt_df.abs().sum()
    keep_alphas = keep_alphas[keep_alphas>0].index.tolist()
    bkt_df = bkt_df[keep_alphas]

    ALPHA_DIR = '%(dir)s/%(freq)s/current/'%{'dir':dir, 'freq':freq}

    big_df = pandas.DataFrame()
    for alpha in keep_alphas:
        raw_alpha_dir = "%s/%s/%s/"%(ALPHA_DIR, univ, alpha)
        fn = raw_alpha_dir+'%s_%s.alp'%(alpha, date_str)
        if os.path.exists(fn):
            tmp_df = pandas.read_csv(fn, header=None, names=['barrid', alpha], index_col=0)
            big_df = big_df.join(tmp_df, how='outer')
        else:
            big_df[alpha] = None

    #the v1 def. for bucket alphas
    b_out_dir = "%s/%s/%s/"%(ALPHA_DIR, univ, 'b_'+bkt+'_eq_all')

    if not os.path.exists(b_out_dir):
        os.makedirs(b_out_dir)
        
    output_df = big_df.fillna(0).mean(axis=1)
    output_df.to_csv('%(dir)s/b_%(bkt)s_eq_all_%(date)s.alp'%{'dir':b_out_dir, 'bkt':bkt, 'date':date_str})

    #the v2 def.
    #add country into big_df
    b_out_dir = "%s/%s/%s/"%(ALPHA_DIR, univ, 'b_'+bkt+'_eq_c')

    if not os.path.exists(b_out_dir):
        os.makedirs(b_out_dir)
    
    if ctry_df is None:
        ctry_df = pandas.DataFrame(lb.loadrsk2(model, 'S', date, daily=True)['COUNTRY'])
    if univ_df is None:
        univ_df = lb.load_production_universe(univ, date)
    
    big_df = big_df[big_df.index.isin(univ_df.index)]
    big_df = big_df.join(ctry_df, how='left')

    output_df2 = pandas.DataFrame()
    for k,v in big_df.groupby('COUNTRY'):
        if k in bkt_df.index:
            keep_alphas = bkt_df.ix[k]
            keep_alphas = keep_alphas[keep_alphas>0].index.tolist()
            output_df2 = pandas.concat([output_df2, pandas.DataFrame(v[keep_alphas].fillna(0).mean(axis=1))])
        
    output_df2.to_csv('%(dir)s/b_%(bkt)s_eq_c_%(date)s.alp'%{'dir':b_out_dir, 'bkt':bkt, 'date':date_str}, header=None)

def backfill_b_1d(date):
    """
    this function is to backfill the bucket signals defined in the production env for 1 day.
    """
    print "generating prod buck alphas for %s"%date
    
    sys.path.insert(0, '/home/dwang/git_new/')
    import nipun.alphagen.alpha_combine as ac
    import nipun_task.derived.gen_icscaling as gi
    
    gi.PREALPHA_DIR = '/home/dwang/work/alpha_files/prealpha'
    gi.run(date - datetime.timedelta(1), 'npxchnpak')
    
    AC_o = ac.AlphaCombination(mname='alpha_v5', date=date, risk_model='ASE1JPN', production=False, opt_pth='/research/production_alphas/daily/current/npxchnpak/', output_b_only=True, exclude_otypes=[])
    AC_o.run(univ='npxchnpak', userpath='/home/dwang/work/alpha_files/prealpha/', country_std=True)

def backfill_b_alphas2(startdate=datetime.datetime(2005,1,1), enddate=datetime.datetime.today() - pandas.datetools.day, ncpus=8):
    """
    this function is to backfill the bucket signals defined in the production env for 1 day.
    """
    print "going to generate production def. bucket alphas"
    job_server = pp.Server(ncpus)
    jobs = []
    for date in pandas.DateRange(startdate, enddate, offset=pandas.datetools.day):
        jobs.append(job_server.submit(backfill_b_1d, (date,), (), ('pandas', 'datetime')))

    for job in jobs:
        job()
    job_server.print_stats()
    job_server.destroy()

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option('--univ', dest='univ', default=None, help='the universe of alphas to copy')
    parser.add_option('--freq', dest='freq', default=None, help='daily or monthly')
    parser.add_option('--n', dest='max_n', default=None, type='int', help='the max number of changed alphas that are going to be processed')
    parser.add_option('--alphas', dest='alphas', default=None, type='str', help='alphas for manual run')
    parser.add_option('--symlk', dest='symlk', default='False', type='str', help='hard copy or create symlink')
    parser.add_option('--start_date', dest='start_date', default=None, type='str', help='start date')
    parser.add_option('--stop_date', dest='stop_date', default=None, type='str', help='stop date')
    parser.add_option('--prod', dest='prod', default=1, type='int', help='PRODUCTION')
    opts, args = parser.parse_args()
    opts.symlk = True if opts.symlk=='True' else False
    opts.start_date = datetime.datetime.strptime(opts.start_date, '%Y%m%d') if opts.start_date else datetime.datetime(2005, 1, 1)
    opts.stop_date = datetime.datetime.strptime(opts.stop_date, '%Y%m%d') if opts.stop_date else None
    univ = opts.univ.split(',')
    
    for uni in univ:
        uni = uni.strip()
        alphas = opts.alphas
        print 'processing alphas for %s' % uni
        if alphas:
            alphas = pandas.DataFrame([[a.strip(),uni] for a in alphas.split(',')], columns=['alpha_file', 'universe_n'])
            
        requeue_alphas(uni, opts.freq, opts.max_n, alphas)

        if uni=='npxchnpak':#currently, only backfill bucket alphas for npxchnpak, alpha_v5
            print "going to generate eq_all and eq_c bucket alphas"
            backfill_b_alphas(univ=uni)

            backfill_b_alphas2()

