#-*- coding: utf-8 -*-
#******************************************************************************
# Copyright of this product 2013-2023,
# MACHBASE Corporation(or Inc.) or its subsidiaries.
# All Rights reserved.
#******************************************************************************

# $Id:$

import re
import json, sys, os, csv
import time
from machbaseAPI import machbase

reload(sys)
sys.setdefaultencoding('utf8')

PROGRAM_ROOT = os.path.dirname( os.path.realpath(__file__) )
if PROGRAM_ROOT[-1] != '/':
    PROGRAM_ROOT += '/'

def sample():
    sPort = int(os.environ['MACHBASE_PORT_NO'])
    sTablename = 'tag'
    sPath = PROGRAM_ROOT + 'sample_data.csv'

    sTags = ['TEMPERATURE', 'RAINFALL', 'WINDSPEED', 'PRESSURE_hPa', 'PRESSURE_SEA', 'HUMIDITY']

    sDb = machbase()
    if sDb.open('127.0.0.1', 'SYS', 'MANAGER', sPort) is 0 :
        return sDb.result()

    sDb.columns(sTablename)
    sColumns = sDb.result()

    if sDb.close() is 0:
        return sDb.result()

# append start
    sTypes = []
    for sItem in re.findall('{[^}]+}', sColumns):
        sTypes.append(json.loads(sItem).get('type'))

    sStart = time.time()

    sDb = machbase()
    if sDb.open('127.0.0.1', 'SYS', 'MANAGER', sPort) is 0 :
        return sDb.result()

    if sDb.appendOpen(sTablename, sTypes) is 0:
        return sDb.result()
    print "append open"

    print "append data...",
    sys.stdout.flush()

    try:
        sCount = 0
        with open(sPath) as sInputFile:
            sLines = csv.reader(sInputFile)
            for sLine in sLines:
                #print sLine
                sValues = []
                for sIdx in range(len(sTags)):
                    sTemp = []
                    sTemp.append(sTags[sIdx])
                    sTemp.append(sLine[1] + ':00')
                    sTemp.append(float(0 if sLine[sIdx+2] == '' else sLine[sIdx+2]))
                    sValues.append(sTemp)
                    sCount += 1
                if sDb.appendData(sTablename, sTypes, sValues) is 0:
                    return sDb.result()
    except:
        e = sys.exc_info()[0]
        print e
        return False

    if sDb.appendClose() is 0:
        return sDb.result()

    if sDb.close() is 0 :
        return sDb.result()
    sEnd = time.time()
    print "append Close (" + str(sCount) + " records)"
    print 'elapsed time : ' + str(sEnd - sStart) + " sec\n"
# append end

    return "successfully executed."

if __name__=="__main__":
    print sample()
