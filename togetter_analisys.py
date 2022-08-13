#!/usr/bin/python2
# -*- coding:utf-8 -*-
import anydbm
import zlib
import logging
import re
logging.basicConfig(level=logging.INFO)

def sampling():
    db=anydbm.open('togetter.dbm')
    new_db = anydbm.open('togetter_sample.dbm','c')
    cnt=0
    for k in db.keys():
        kk=k.split('_')[1]
        if not re.search('^20.*-01-01',kk):continue
        body = zlib.decompress(db[k])
        if cnt%1000==0:
            logging.info("cnt={} k={} body={}".format(cnt,k,body))
        new_db[k]=db[k]
        cnt+=1
        #if cnt>10000:
        #    break
    new_db.close()
def get_info():
    db = anydbm.open('togetter_sample.dbm')
    for k in db.keys():
        print(k)
def main():
    sampling()
    #get_info()
    
if __name__=='__main__':main()


    
