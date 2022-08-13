#!/usr/bin/python2
# -*- coding:utf-8 -*-
#import requests
from datetime import datetime
import urllib2
import sqlite3
import xml.etree.ElementTree as ET
import xml.etree.ElementTree
import lxml.html
from io import StringIO
from datetime import datetime
import re
import logging
logging.basicConfig(level=logging.INFO)

srcs=(#('index','http://togetter.com/rss/index'),\
    ('recent','http://togetter.com/rss/recent'),
    ('recentpopular','https://togetter.com/rss/recentpopular'),
    ('hot','https://togetter.com/rss/hot'),
)
    
class Item(object):
    def __init__(self,dbname):
        self.conn = sqlite3.connect(dbname)
        self.cursor = self.conn.cursor()
    def __del__(self):
        if self.conn:
            self.conn.commit()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    def __conv_item(self,i):
        dkt={}
        for k in ('guid','title','link','pubDate','description'):
            elm = i.find(k)
            if elm is not None:
                dkt[k] = elm.text
            else:
                dkt[k] = None
        if dkt['guid'] is None:
            dkt['guid'] = dkt['link']
        return dkt
    def __conv_desc(self,desc):
        dkt={}
        if desc:
            doc = lxml.html.parse(StringIO(unicode(desc)))
            vv =doc.xpath('.//div/div')[0]
            for i,k in enumerate(vv.itertext()):
                if i==1:
                    dkt['author']=k
                elif i==5:
                    k=k.strip()
                    aa=re.findall("fav:([\d]+) view:([\d]+)",k)
                    if aa:
                        dkt['fav']=int(aa[0][0])
                        dkt['view']=int(aa[0][1])
        return dkt
    def __conv_pub_date(self,pubdate):
        if pubdate:
            # print(pubdate)
            pubdate=re.sub(' \+0900','',pubdate)
            return datetime.strptime(pubdate,"%a, %d %b %Y %H:%M:%S")
        else:
            return None
    def __conv_list_base(self,dkt):
        return [
            dkt['guid'],
            dkt['link'],
            dkt['title'],
            dkt['pub_date'].strftime("%Y-%m-%d %H:%M:%S"),
            dkt['description'],
            dkt['type'],
            dkt['author']
        ]
    def __conv_list_view(self,dkt):
        return [
            dkt['link'],
            dkt['view'],
            dkt['fav'],
            dkt['fetched_at']
        ]
    def conv(self,root,tp,dt):
        dts=[]
        dts2=[]
        for i in root.findall('./channel/item'):
            dkt = self.__conv_item(i)
            dkt2 = self.__conv_desc(dkt['description'])
            dkt['pub_date'] = self.__conv_pub_date(dkt['pubDate'])
            for k in ('author','fav','view'):
                v=dkt2.get(k,None)
                dkt[k]=v
            #print(dkt)
            dkt['type']=tp
            dkt['fetched_at']=dt
            dls=self.__conv_list_base(dkt)
            dts.append(dls)
            if dkt['view']:
                dls2=self.__conv_list_view(dkt)
                dts2.append(dls2)
        return dts,dts2
    def insert_base_data(self,data):
        link=data[1]
        sql='select count(1) as cnt from togetter_base where link = ?'
        self.cursor.execute(sql,[link])
        r=self.cursor.fetchone()
        logging.info("link={} r={}".format(link,r))
        if r[0]==0:
            sql='INSERT INTO togetter_base(guid,link,title,pub_date,description,type,author) VALUES (?,?,?,?,?,?,?);'
            self.cursor.execute(sql,data)
    def insert_view_data(self,data):
        sql='INSERT INTO view_fav(link,view,fav,fetched_at) VALUES (?,?,?,?);'
        self.cursor.executemany(sql,data)
def get_data(conn,key,url,date):
    r=urllib2.urlopen(url)
    if r:
        c=r.read()
        root=ET.fromstring(c)
        i=Item(conn)
        dts,dts2=i.conv(root,key,date)
        for d in dts:
            i.insert_base_data(d)
        if dts2:
            i.insert_view_data(dts2)
def get_togetter():
    conn='togetter.sqlite'
    for key,url in srcs:
        now=datetime.now()
        get_data(conn,key,url,now)

def main():
    get_togetter()

if __name__=='__main__':main()
