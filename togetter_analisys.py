#!/usr/bin/python2
# -*- coding:utf-8 -*-
import anydbm
import zlib
import logging
import re
from collections import Counter
import sqlite3
import xml.etree.ElementTree as ET
import xml.etree.ElementTree
import lxml.html
from io import StringIO
from datetime import datetime

logging.basicConfig(level=logging.INFO)

    
def sampling():
    db = anydbm.open('togetter.dbm')
    new_db = anydbm.open('togetter_sample.dbm','n')
    cnt=0
    for k in db.keys():
        kk=k.split('_')[1]
        if not re.search('^20[\d]{2}-[\d]{2}-01',kk):continue
        #body = zlib.decompress(db[k])
        if cnt%1000==0:
            logging.info("cnt={} k={}".format(cnt,k))
        new_db[k]=db[k]
        cnt+=1
        #if cnt>10000:
        #    break
    new_db.close()
class Item(object):
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
                #print(u"i={} k={}".format(i,k))
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
    def __conv_list(self,dkt):
        #(guid text,link text,title text,pubDate text,description text,type text,fetched_at text,view integer,fav integer,author text);

        return [
            dkt['guid'],
            dkt['link'],
            dkt['title'],
            dkt['pub_date'].strftime("%Y-%m-%d %H:%M:%S"),
            dkt['description'],
            dkt['type'],
            dkt['fetched_at'].strftime("%Y-%m-%d %H:%M:%S"),
            dkt['view'],
            dkt['fav'],
            dkt['author']
        ]
    def conv(self,root,tp,dt):
        dts=[]
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
            dls=self.__conv_list(dkt)
            dts.append(dls)
        return dts
def init_db(fname):
    conn = sqlite3.connect(fname)
    cursor = conn.cursor()
    return conn,cursor
def create_table(conn,cursor):
    sql='''
CREATE TABLE IF NOT EXISTS togetter
(id integer primary key autoincrement,guid text,link text,title text,pub_date text,description text,type text,fetched_at text,view integer,fav integer,author text);
    '''
    cursor.execute(sql)
    conn.commit()
def ins_sql():
    return '''
insert into togetter (guid,link,title,pub_date,description,type,fetched_at,view,fav,author) values (?,?,?,?,?,?,?,?,?,?)
    '''
def ins_info():
    db = anydbm.open('togetter.dbm')
    conn,cursor = init_db('togetter.sqlite')
    create_table(conn,cursor)
    ins_s=ins_sql()
    def conv_dt(dt):
        return datetime.strptime(dt,"%Y-%m-%d:%H-%M-%S")
    cnt = 0
    for k in db.keys():
        tp,dt=k.split('_')
        dt=conv_dt(dt)
        body=zlib.decompress(db[k])
        try:
            root=ET.fromstring(body)
            i=Item()
            dts=i.conv(root,tp,dt)
            cursor.executemany(ins_s,dts)
            conn.commit()
            cnt+=1
            if cnt % 5000 == 0:
                logging.info("cnt={} k={}".format(cnt,k))
        except xml.etree.ElementTree.ParseError as err:
            pass
    conn.commit()
    cursor.close()
    conn.close()
def main():
    #sampling()
    ins_info()
    
if __name__=='__main__':main()


    
