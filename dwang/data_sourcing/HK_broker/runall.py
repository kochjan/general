#!/opt/python2.7/bin/python2.7
from web_scraper import *
from abci import Abci
from citics import Citics
from cpy import Cpy
from dbs import Dbs
from kgi import Kgi
from poems1 import Poems1
from poems2 import Poems2
from publicsec import Publicsec
from utrade import Utrade

if __name__='__main__':

    opts, args = parse()

    for clss in [Abci,Citics, Cpy, Dbs, Kgi]
    try:
        obj = clss(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if opts.email: obj.email()
