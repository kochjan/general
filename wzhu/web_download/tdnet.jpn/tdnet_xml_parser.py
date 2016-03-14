#!/opt/anaconda/py/bin/python

# TDnet XML parser
# TDnet changed from <name attrs>.xml to <ix:type id=name attrs>.htm on 20140113
# The newer format offers easier decoding (_-separated attrs) and divide values into NonNumeric and NonFractions
# The newer one has scale, while the older one need not worry about the scaling of large numbers
# The tag names do not collide, so we can process both in the same code

# note: soup turns all attrKey to lowercase
import bs4
import re

sName = 'name'

sContext = 'contextref'
ignoreNonNumVal = ['CurrentYearInstant', 'CurrentYearDuration','CurrentAccumulated']
nonNumericExpected = {sContext: ignoreNonNumVal}

nilKey = 'xsi:nil'
sTrue = 'true'
sFalse= 'false'
sBool = 'ixt:boolean'

# nonFraction
sFormat = 'format'
sDecimal = 'decimals'
sScale = 'scale'
sUnit = 'unitref'

expDecimal = ['2','3','-6']
expFormat = ['ixt:numdotdecimal']
expScale = ['0','6','-2']
expUnit = ['JPYPerShares','JPY','pure']
nonFractionExpected = {sFormat:expFormat, sDecimal:expDecimal, sScale:expScale}

# context
sid = 'id'
(sbeginDate, sendinDate, slocalID) = ['beginDate','endinDate','localID',]

digits = "0123456789"
# number pattern: ends in a number
def numTest(nStr):
    # unicode fails for re.compile('\d$')
    # test with: nonempty, end in digit, avoid phone but keep negative
    return (len(nStr)>0) and (nStr[-1] in digits) and ('-' not in nStr[1:]) and (len(nStr) < 20)

'''
dim1Test = re.compile('(prior|current)year', re.IGNORE_CASE)
dim2Test = re.compile('(duration|instant)', re.IGNORE_CASE)
dim3Test = re.compile('(non|)consolidatedmember', re.IGNORE_CASE)
dim4Test = re.compile('(previous|current)member', re.IGNORE_CASE)
dim5Test = re.compile('(forecast|result)member', re.IGNORE_CASE)
'''

def normContextStr(cstr):
    '''old and new format express context string differently. this normalize them into a common theme:
    Dimensions:
        Prior vs Current -Year
        Duration vs Instant
        NonConsolidated vs Consolidated
        current vs previous (estimate)
        forecast vs result
        
        currentyearduration_nonconsolidatedmember_currentmember_forecastmember
    
    
    outStr = cstr.lower()
    if '_' in outStr:
        # TODO: 5 dimensional tests
        pass
        
    '''
    return cstr
    
def xml2map(file):
    ''' the grand xml parser that interprets both old and new TDnet format. converting an xml/htm file to a dictionary'''
    resM = {}
    xml=open(file, 'r').read()
    soup= bs4.BeautifulSoup(xml)
    
    # nonNumeric ix are: name, context, format, nil, unitRef
    for ix in soup.findAll('ix:nonnumeric'):
        keys = ix.attrs.keys()
        # make sure name is in field
        if sName not in keys:
            print "unexpected noname ix"
            print ix
            print
            continue
        longname = ix[sName]
        # context are basically currentYear
        if sContext in keys:
            strContext = ix[sContext]
            # this stanza is not necessary, but prints out unexpected prefix
            if strContext not in ignoreNonNumVal:
                # could be a long context prefixed by time
                unseenPrefix = True
                for prefix in ignoreNonNumVal:
                    if strContext.startswith(prefix):
                        unseenPrefix=False
                        break
                if unseenPrefix:
                    print 'unexpected context'
                    print ix
                    print
                    continue
            # to keep in sync with older format, we will add context to varname
            longname += ':'+normContextStr(ix[sContext])
        # null value
        if nilKey in keys:
            boolV = ix[nilKey]
            if boolV.endswith(sTrue):
                resM[longname]=None
            else:
                print "unexpected nil boolVal"
                print boolV
            continue
        # boolean
        if sFormat in keys and ix[sFormat].startswith(sBool):
            resM[longname]= ix[sFormat].endswith(sTrue) 
            continue
        resM[longname] = ix.text
     
    # nonFraction ix are: name, context, format, nil unitRef, decimal
    for ix in soup.findAll('ix:nonfraction'):
        keys = ix.attrs.keys()

        # make sure name is in field
        if sName not in keys:
            print "unexpected noname ix"
            print ix
            print
            continue
        longname = ix[sName]
        # context should become part of varName
        if sContext in keys:
            longname += ':'+normContextStr(ix[sContext])
            
        # null value
        if nilKey in keys:
            boolV = ix[nilKey]
            if boolV.endswith(sTrue):
                resM[longname]=None
            else:
                print "unexpected nil boolVal"
                print boolV
            continue
        scale = 1
        if sScale in keys:
            try:
                scaleInt = (int)(ix[sScale])
                # if (scaleInt not in [0,6]):
                # we only scale result if they are not unit or million
                # NB: not a good idea. should have viewing properties separately
                scale = 10**scaleInt
            except:
                print "unexpected scale: ",ix[sScale]

        # validate expected behaviors
        for (key, expVal) in nonFractionExpected.iteritems():
            if key in keys and ix[key] not in expVal:
                print "unexected key: ",key, " value: ",ix[key]
        
        sValue = ix.text.replace(',','')
        try:
            value = float(sValue) * scale
        except:
            value = sValue
            print "unexpected nonFraction value:", value
        resM[longname] = value
    
    # contexts: start/endDate
    # this behavior is shared by both new and old xml formats
    for cx in soup.findAll('xbrli:context'):
        keys = cx.attrs.keys()
        if sid not in keys:
            print "unexpected context no id"
            print cx
            continue
        varstr = cx[sid]
        lid = cx.find('xbrli:identifier')
        resM[varstr+':'+slocalID] = lid.text if lid else None
        sdate = cx.find('xbrli:startdate')
        resM[varstr+':'+sbeginDate] = sdate.text if sdate else None
        edate = cx.find('xbrli:enddate')
        resM[varstr+':'+sendinDate] = edate.text if edate else None
        
        #Note: for the scenario section, the explicitMember dimensions are just the split of context id string 
        # but if this assumption is ever changed, we need to make sure the id attribute is useful

    # oldFormat
    for ix in soup.findAll(re.compile("^tse-t.*")):
        keys = ix.attrs.keys()
                
        longname = ix.name
        # context are basically currentYear
        if sContext in keys:
            longname += ':'+normContextStr(ix[sContext])
        # null value
        if nilKey in keys:
            boolV = ix[nilKey]
            if boolV.endswith(sTrue):
                resM[longname]=None
            else:
                print "unexpected nil boolVal"
                print boolV
            continue
            
        # validate expected behaviors
        for (key, expVal) in nonFractionExpected.iteritems():
            if key in keys and ix[key] not in expVal:
                print "unexpected key: ",key, " value: ",ix[key]
                
        # old format has no scale or number issues
        sValue = ix.text
        # check for boolean
        if sValue == sTrue:
            sValue = True
        elif sValue == sFalse:
            sValue = False
        elif numTest(sValue):
            try:
                sValue = float(sValue)
            except:
                print "unexpected numberPatt:", sValue
            
        resM[longname] = sValue
        
    return resM

duplicableTagPrefix = ['CurrentYearDuration','CurrentYearPeriod','PriorYearDuration','PriorYearPeriod',]
duplicableTagSuffix = [sbeginDate,sendinDate,slocalID]
def cleanNullFalseDup(mapM):
    ''' 
    a clean view of relevant entrie:
    ignore null and false values
    reduce the multiple copies of duplicableTagPrefix:Suffix, e.g. CurrentYearDuration:XXXXXXXX:beginDate
    '''
    newM = {}
    oldKeys = mapM.keys()
    for (k,v) in mapM.iteritems():
        # ignore null and false values
        if v: 
            # test for duplicated duration information
            nameSplit = k.split('_')
            if len(nameSplit) > 1: # 1st requirement for skipping
                prefix = nameSplit[0]
                if prefix in duplicableTagPrefix: #2nd requirement for skipping
                    last = nameSplit[-1]
                    lastSplit = last.split(':')
                    if len(lastSplit) ==2: # 3rd requirement
                        suffix = lastSplit[1]
                        if suffix in duplicableTagSuffix:
                            #now we are ready to test for value
                            candidateKey = "{}:{}".format(prefix,suffix)
                            if candidateKey in oldKeys:
                                if (mapM[candidateKey] == v):
                                    # time/id info repeated 
                                    continue
            #if not duplicated, then we keep it
            newM[k] = v
        
            
    return newM

def mapGrep(mapV, patt):
    ''' grep only keys with certain words'''
    newM = {}
    for (k,v) in mapV.iteritems():
        if patt in k:
            newM[k] = v
    return newM

def normalizeMap(mapV):
    '''keys should be normalized to lowercase, tse-t-ed vs tse-ed-t, etc'''
    newM = {}
    for (k,v) in mapV.iteritems():
        # lowercase
        newKey = k.lower()
        # tse-t-ed vs tse-ed-t
        if ':' in newKey:
            parts = newKey.split(':')
            if parts[0].startswith('tse-'):
                parts[0] = 'ed' if 'ed' in parts[0] else 'rv'
            newKey = ':'.join(parts)
        
        newM[newKey] = v
    return newM
    
def easyViewMap(mapV):
    ''' turn large numbers into units of Millions'''
    for k in sorted(mapV):
        v = mapV[k]
        if isinstance(v, float):
            if (abs(v) > 10**6):
                v = "{} M".format(v/10**6)
        print k, ' : ', v
        
from collections import Counter
def showKeyComponents(keyList):
    l = []
    for x in keyList:
        l.extend(x.split(':'))

    counts = Counter(l)
    i=1
    for k in sorted(counts.keys()):
        print i, k,counts[k]
        i += 1
    
def showKeyComponentsByFreq(keyList):
    l = []
    for x in keyList:
        l.extend(x.split(':'))

    counts = Counter(l)
    i=1
    for k in counts.most_common():
        print i, k
        i += 1
   
def viewFile(file):
    return  easyViewMap(normalizeMap(cleanNullFalseDup(xml2map(file))))

import sys
def main():
    if len(sys.argv)!=2:
        print "usage: tdnet_xml_parser.py xbrlFile"
        return
    viewFile(sys.argv[1])

if __name__ == '__main__':
    main()
