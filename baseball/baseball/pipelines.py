# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
from scrapy.exceptions import DropItem

import mysql.connector
import os

class BaseballPipeline(object):

    CREATE_TABLE_BATTER ="""
    CREATE TABLE if not exists batter (
      id integer auto_increment primary key,
      year integer,
      name text ,
      team text ,
      bat text ,
      games integer ,
      pa integer ,
      ab integer ,
      r integer ,
      h integer ,
      doub integer ,
      triple integer ,
      hr integer ,
      tb integer ,
      rbi integer ,
      so integer ,
      bb integer ,
      ibb integer ,
      hbp integer ,
      sh integer ,
      sf integer ,
      sb integer ,
      cs integer ,
      dp integer ,
      ba real ,
      slg real ,
      obp real,
      ops real,
      rc real,
      rc27 real,
      create_date date,
      update_date date
    ) 
    """

    CREATE_TABLE_PITCHER ="""
    CREATE TABLE if not exists pitcher (
      id integer auto_increment primary key,
      year integer,
      name text ,
      team text ,
      throw text ,
      games integer ,
      w integer ,
      l integer ,
      sv integer ,
      hld integer ,
      hp integer ,
      cg integer ,
      sho integer ,
      non_bb integer ,
      w_per real ,
      bf integer ,
      ip real ,
      h integer ,
      hr integer ,
      bb integer ,
      ibb integer ,
      hbp integer ,
      so integer ,
      wp integer ,
      bk integer ,
      r integer ,
      er integer ,
      era real ,
      create_date date,
      update_date date
    ) 
    """

    INSERT_BATTER = """
    insert into batter(
    year, 
    name, 
    team, 
    bat, 
    games, 
    pa, 
    ab, 
    r, 
    h, 
    doub, 
    triple, 
    hr, 
    tb, 
    rbi, 
    so, 
    bb, 
    ibb, 
    hbp, 
    sh, 
    sf,
    sb,
    cs,
    dp,
    ba,
    slg,
    obp,
    ops,
    rc,
    rc27,
    create_date,
    update_date
    ) 
    values(
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    now(), 
    now()
    )
    """

    INSERT_PITCHER = """
    insert into pitcher(
    year, 
    name, 
    team, 
    throw, 
    games, 
    w, 
    l, 
    sv, 
    hld, 
    hp, 
    cg, 
    sho, 
    non_bb, 
    w_per, 
    bf, 
    ip, 
    h, 
    hr, 
    bb, 
    ibb, 
    hbp, 
    so,
    wp,
    bk,
    r,
    er,
    era,
    create_date,
    update_date
    ) 
    values(
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, 
    now(), 
    now()
    )
    """

    DATABASE_NAME = 'baseball'
    DATABASE_USER = os.environ.get('MYSQL_USER')
    DATABASE_PASSWORD = os.environ.get('MYSQL_PASSWORD')
    conn = None

    def __init__(self):
        """
        Tableの有無をチェック,無ければ作る
        """
        conn = mysql.connector.connect(username=self.DATABASE_USER, password=self.DATABASE_PASSWORD,
                                       host='localhost')
        cursor = conn.cursor(buffered=True)
        try:
            cursor.execute("create database if not exists {} default character set 'utf8'".format(self.DATABASE_NAME))
            cursor.execute("use {}".format(self.DATABASE_NAME))
        except mysql.connector.Error as err:
            print(err)

        if cursor.execute("show tables like 'batter';") is None:
            cursor.execute(self.CREATE_TABLE_BATTER)
        if cursor.execute("show tables like 'pitcher';") is None:
            cursor.execute(self.CREATE_TABLE_PITCHER)
        cursor.close()

    def open_spider(self, spider):
        """
        初期処理(DBを開く)
        :param spider: ScrapyのSpiderオブジェクト
        """
        self.conn = mysql.connector.connect(username=self.DATABASE_USER, password=self.DATABASE_PASSWORD,
                                       host='localhost', database=self.DATABASE_NAME)
        self.cursor = self.conn.cursor(buffered=True)

    def process_item(self, item, spider):
        """
        成績をSQLite3に保存
        :param item: Itemの名前
        :param spider: ScrapyのSpiderオブジェクト
        :return: Item
        """
        # Spiderの名前で投入先のテーブルを判断
        if spider.name == 'batter':
            # 打者成績
            self.cursor.execute(self.INSERT_BATTER,(
                item['year'], item['name'], item['team'], item['bat'], item['games'], item['pa'], item['ab'], item['r'],
                item['h'], item['double'], item['triple'], item['hr'], item['tb'], item['rbi'], item['so'], item['bb'],
                item['ibb'], item['hbp'], item['sh'], item['sf'], item['sb'], item['cs'], item['dp'], item['ba'],
                item['slg'], item['obp'], item['ops'], item['rc'], item['rc27'],
            ))
        elif spider.name == 'pitcher':
            # 投手成績
            self.cursor.execute(self.INSERT_PITCHER,(
                item['year'], item['name'], item['team'], item['throw'], item['games'], item['w'], item['l'],
                item['sv'], item['hld'], item['hp'], item['cg'], item['sho'], item['non_bb'], item['w_per'], item['bf'],
                item['ip'], item['h'], item['hr'], item['bb'], item['ibb'], item['hbp'], item['so'], item['wp'],
                item['bk'], item['r'], item['er'], item['era'],
            ))
        else:
            raise DropItem('spider not found')
        self.conn.commit()
        return item

    def close_spider(self, spider):
        """
        終了処理(DBを閉じる)
        :param spider: ScrapyのSpiderオブジェクト
        """
        self.cursor.close()
        self.conn.close()
