import re
import pandas
import subprocess
from datetime import datetime as dd
import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

MATCH_LEN = 34

def parse_mcap(date):

    try:
        fname = '{:%Y%m}.pdf.txt'.format(date)
	# do the preprocessing in shell
        # proc = subprocess.Popen("pdftotext -raw %s -  | perl -pe 's/[^[:ascii:]]//g; s/(\d),(\d)/\1\2/g; s/^\s+//;' " % fname, cwd='mcap/', shell=True, stdout=subprocess.PIPE)
	proc = subprocess.Popen("cat {}".format(fname), cwd='mcap.txt/', shell=True, stdout=subprocess.PIPE)
        raw = proc.stdout.read()
    except Exception,e :
        print e
        return

    replace = { #'\nWarehousing & Harbor\nTransportation Services': ' Warehousing & Harbor Transport Service', \
		'Fishery,Agriculture':'Fishery Agriculture',
		'\nWarehousing & Har':' Warehousing & Har',
		'bor\nTransportation Services':'bor Transport Service'}

    for repl in replace.keys():
        raw = raw.replace(repl, replace[repl])
    raw = raw.split('\n')

    logging.debug('lines = {}'.format(len(raw)))
    
    if len(raw) == 0:
        return
    output = []
    started = False
    ended = False
    for row in raw:
	if not started:
	    if '1stSection' in row:
		started = True
	elif not ended:
	    if '2ndSection' in row:
		ended = True
	    else: #started but not yet ended, we keep data
		cols = []
		tokens = row.split(' ')
		cols.append(tokens[0])
		cols.append(tokens[1])
		cols.append(" ".join(tokens[2:]))
	        output.append(cols) 
    
    col_names = ['company_ct', 'mcap', 'sector']

    data = pandas.DataFrame(output, columns=col_names)
    
    data['date'] = date.strftime('%Y%m%d')

    data.to_csv('mcap.csv/{:%Y%m}.csv'.format(date), index=False, header=False)
    return

#for dt in pandas.DateRange(datetime.datetime(2014,6,1), datetime.datetime(2014,6,2)):
for y in range(2010, 2016):
    end = 13
    if y==2015:
	end=7
    for m in range(1, end):
        parse_mcap(dd(y,m,1))
