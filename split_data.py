#!/usr/bin/python2
# -*- coding:utf-8 -*-
import logging

import sqlite3

logging.basicConfig(level=logging.INFO)

    

def init_db(fname):
    conn = sqlite3.connect(fname)
    cursor = conn.cursor()
    return conn,cursor
def create_table(conn,cursor):
    def create(sql,conn,cursor):
        cursor.execute(sql)
        conn.commit()
    sql='''
CREATE TABLE IF NOT EXISTS togetter_base(id integer primary key autoincrement,guid text,link text,title text,pub_date text,description text,type text,author text);

    '''
    create(sql,conn,cursor)

    sql='''
CREATE TABLE IF NOT EXISTS view_fav(id integer primary key autoincrement,link text,view integer,fav integer,fetched_at text);
    '''
    create(sql,conn,cursor)
def ins_sql():
    return '''
insert into togetter (guid,link,title,pub_date,description,type,fetched_at,view,fav,author) values (?,?,?,?,?,?,?,?,?,?)
    '''
def exists_key(type,dt,cursor):
    sql='''
select count(1) from togetter where type=? and fetched_at=?
    '''
    cursor.execute(sql,(type,dt.strftime('%Y-%m-%d %H:%M:%S')))
    a=cursor.fetchone()
    result=False
    if a:
        cnt=a[0]
        if cnt>0:
            result=True
    logging.debug("result={} cnt={} tp={} dt={}".format(result,a,type,dt))
    return result
class SplitData(object):
    def __init__(self,r):
        self.id = r[0]
        self.guid = r[1]
        self.link = r[2]
        self.title = r[3]
        self.pub_date = r[4]
        self.description = r[5]
        self.type = r[6]
        self.fetched_at = [r[7]]
        self.view = [r[8]]
        self.fav = [r[9]]
        self.author = r[10]
    def add(self,r):
        self.fetched_at.append(r[7])
        self.view.append(r[8])
        self.fav.append(r[9])
    def gen_base(self):
#            sql='''
#CREATE TABLE IF NOT EXISTS togetter_base(id integer primary key autoincrement,guid text,link text,title text,pub_date text,description text,type text,author text);
#
#    '''
        sql='INSERT INTO togetter_base (guid,link,title,pub_date,description,type,author) VALUES (?,?,?,?,?,?,?);'
        v=[(self.guid,self.link,self.title,self.pub_date,self.description,self.type,self.author),]
        return sql,v
    def gen_view(self):
#CREATE TABLE IF NOT EXISTS view_fav(id integer primary key autoincrement,link text,view integer,fav integer,fetched_at text);
        sql='INSERT INTO view_fav (link,view,fav,fetched_at) VALUES (?,?,?,?);'
        vl=[]
        for f,v,fav in zip(self.fetched_at,self.view,self.fav):
            vl.append((self.link,v,fav,f))
        return sql,vl
    def sm(self):
        return sum(self.fav)
    def __str__(self):
        return 'link={} fetched_at={} view={} fav={}'.format(self.link,self.fetched_at[:3],self.view[:3],self.fav[:3])

class GetInfo(object):
    def __init__(self,conn,cursor,new_conn,new_cursor):
        self.conn = conn
        self.cursor = cursor
        self.new_conn = new_conn
        self.new_cursor = new_cursor
    def __del__(self):
        if self.new_conn:
            self.new_conn.commit()
        if self.new_cursor:
            self.new_cursor.close()
        if self.new_conn:
            self.new_conn.close()
    def get_info(self):
        sql='''
select link from togetter group by link limit 100000;
        '''
        self.cursor.execute(sql)
        for cnt,r in enumerate(self.cursor.fetchall()):
            s2='''
select * from togetter where link=? order by fetched_at desc
            '''
            self.cursor.execute(s2,r)
            sp=None
            for i,r2 in enumerate(self.cursor.fetchall()):
                if i==0:
                    sp = SplitData(r2)
                else:
                    sp.add(r2)
            sql,v = sp.gen_base()
            if v:
                self.new_cursor.executemany(sql,v)
            sql,v = sp.gen_view()
            if v:
                self.new_cursor.executemany(sql,v)
            self.new_conn.commit()
def split_info():
    conn,cursor = init_db('togetter.sqlite')
    new_conn,new_cursor = init_db('togetter2.sqlite')
    create_table(new_conn,new_cursor)
    g = GetInfo(conn,cursor,new_conn,new_cursor)
    g.get_info()

def main():
    split_info()
    
if __name__=='__main__':main()


    
