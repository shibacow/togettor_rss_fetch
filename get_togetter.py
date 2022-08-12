#!/usr/bin/python2
# -*- coding:utf-8 -*-
#import requests
from datetime import datetime
import zlib
import anydbm
import urllib2
srcs=(#('index','http://togetter.com/rss/index'),\
    ('recent','http://togetter.com/rss/recent'),
    ('recentpopular','https://togetter.com/rss/recentpopular'),
    ('hot','https://togetter.com/rss/hot'),
)
def get_data(db,key,url,date):
    n=date.strftime('%Y-%m-%d:%H-%M-%S')
    k="%s_%s" % (key,n)
    print url
    r=urllib2.urlopen(url)
    if r:
        c=r.read()
        #print(c)
        db[k]=zlib.compress(c)

def get_togetter():
    db=anydbm.open('togetter.dbm','c')
    for key,url in srcs:
        now=datetime.now()
        get_data(db,key,url,now)
    db.close()
def get_verify():
    db=anydbm.open('togetter.dbm')
    for k in db.keys():
        c=zlib.decompress(db[k])
        print k,len(c)
    db.close()
    
def main():
    get_togetter()
    #get_verify()
if __name__=='__main__':main()
