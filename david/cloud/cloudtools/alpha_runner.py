

import os
import imp
import pandas
import datetime
from optparse import OptionParser
from dateutil.parser import parse 

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option('--file', dest='file', help='file to run on')
    parser.add_option('--date', dest='date', help='date to run')
    parser.add_option('--daily', dest='daily', default=False, action='store_true', help='run daily')
 
    opts, _ = parser.parse_args()
    try:
        dt = os.environ['MONTH']
        if len(dt) < 10:
            dt += '-01'
        date = parse(dt)
    except Exception, e:
        import traceback
        print traceback.format_exc()
        print os.environ
        date = parse(opts.date)
    
    modname = os.path.basename(opts.file).replace('.py', '')
   
    #TODO: add inputs for outdir, user running, etc. 
    mod = imp.load_source(modname, opts.file)

    if opts.daily:
        the_date = date
        while the_date <= date+pandas.datetools.MonthEnd():
            mod.run(the_date)
            the_date += datetime.timedelta(1)

    else:
        mod.run(date+pandas.datetools.MonthEnd())

