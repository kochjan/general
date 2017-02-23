# for sleep:
import time
import random
def pause(mean=5):
    '''sleeps with normal distribution(mean, std=0.5) seconds '''
    r = 0
    while (r < 0.3):
    # make sure sleeptime positive, and not negligible
        r = random.normalvariate(mean,0.5)
        mean +=1
    time.sleep(r)
    return  

import codecs
def rowsFromUniFile(filepath):
    '''dealing with utf8 files'''
    with open(filepath, 'rb', encoding='utf8') as f:
        rows = f.readlines()
    return rows

