import os
import glob
import pandas
import nipun.cpa.cpa_pickler as pcp

OUTPTH = '/opt/data/nipun_derived/static/alpv5/'

def load_files(tag):
    return glob.glob(OUTPTH+'*{tag}*.pckz'.format(tag=tag))

def summarize(tag):

    fns = load_files(tag)
    print fns
    outdata = pandas.DataFrame()
    i = 0
    for fn in fns:
        print i, len(fns)
        base_name = os.path.basename(fn)
        raw = pcp.read_archive(fn)
        if raw is None: 
            i += 1
            continue
        if isinstance(raw, dict) and len(raw.keys()) == 0:
            i += 1
            continue
            
        try:    
            tmp = pandas.DataFrame([{'date':k, base_name:raw[k]['portfolio']['ret'][0,1]} for k in raw.keys()])
        except:
            print fn
            i += 1
            continue
        tmp.set_index('date', inplace=True)
        outdata = pandas.merge(outdata, tmp, left_index=True, right_index=True, how='outer')
        i += 1
    return outdata
