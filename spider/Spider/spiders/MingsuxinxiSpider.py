# # -*- coding: utf-8 -*-

# 数据爬取文件

import scrapy
import pymysql
import pymssql
from ..items import MingsuxinxiItem
import time
from datetime import datetime,timedelta
import datetime as formattime
import re
import random
import platform
import json
import os
import urllib
from urllib.parse import urlparse
import requests
import emoji
import numpy as np
from DrissionPage import Chromium
import pandas as pd
from sqlalchemy import create_engine
from selenium.webdriver import ChromeOptions, ActionChains
from scrapy.http import TextResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import pandas as pd
from sqlalchemy import create_engine
from selenium.webdriver import ChromeOptions, ActionChains
from scrapy.http import TextResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
# 名宿信息
class MingsuxinxiSpider(scrapy.Spider):
    name = 'mingsuxinxiSpider'
    spiderUrl = 'https://www.muniao.com/beijing/null-0-0-0-0-0-0-0-{}.html?start_date=2025-02-21&end_date=2025-02-22&tn=mn19091015'
    start_urls = spiderUrl.split(";")
    protocol = ''
    hostname = ''
    realtime = False


    def __init__(self,realtime=False,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.realtime = realtime=='true'

    def start_requests(self):

        plat = platform.system().lower()
        if not self.realtime and (plat == 'linux' or plat == 'windows'):
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'h45be198_mingsuxinxi') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return
        pageNum = 1 + 1

        for url in self.start_urls:
            if '{}' in url:
                for page in range(1, pageNum):

                    next_link = url.format(page)
                    yield scrapy.Request(
                        url=next_link,
                        callback=self.parse
                    )
            else:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )

    # 列表解析
    def parse(self, response):
        _url = urlparse(self.spiderUrl)
        self.protocol = _url.scheme
        self.hostname = _url.netloc
        plat = platform.system().lower()
        if not self.realtime and (plat == 'linux' or plat == 'windows'):
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'h45be198_mingsuxinxi') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return
        list = response.css('ul#Lmain_con li.Lcon1')
        for item in list:
            fields = MingsuxinxiItem()
            if '(.*?)' in '''a.s_mn_house_t1::text''':
                try:
                    fields["title"] = str( re.findall(r'''a.s_mn_house_t1::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["title"] = str( self.remove_html(item.css('''a.s_mn_house_t1::text''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''img.ckwebp2::attr(data-original)''':
                try:
                    fields["imgurl"] = str('https:'+ re.findall(r'''img.ckwebp2::attr(data-original)''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["imgurl"] = str('https:'+ self.remove_html(item.css('''img.ckwebp2::attr(data-original)''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''div.s_mn_house_introduction p span:nth-child(1)::text''':
                try:
                    fields["huxing"] = str( re.findall(r'''div.s_mn_house_introduction p span:nth-child(1)::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["huxing"] = str( self.remove_html(item.css('''div.s_mn_house_introduction p span:nth-child(1)::text''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''div.s_mn_house_introduction p span:nth-child(2)::text''':
                try:
                    fields["chuzutype"] = str( re.findall(r'''div.s_mn_house_introduction p span:nth-child(2)::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["chuzutype"] = str( self.remove_html(item.css('''div.s_mn_house_introduction p span:nth-child(2)::text''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''div.s_mn_house_introduction p span:nth-child(3)::text''':
                try:
                    fields["yizhu"] = str( re.findall(r'''div.s_mn_house_introduction p span:nth-child(3)::text''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["yizhu"] = str( self.remove_html(item.css('''div.s_mn_house_introduction p span:nth-child(3)::text''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''div.list_address''':
                try:
                    fields["address"] = str( re.findall(r'''div.list_address''', item.extract(), re.DOTALL)[0].strip().replace('地址：',''))

                except:
                    pass
            else:
                try:
                    fields["address"] = str( self.remove_html(item.css('''div.list_address''').extract_first()).replace('地址：',''))

                except:
                    pass

            if '(.*?)' in '''div.s_mn_house_price2 span::text''':
                try:
                    fields["mwprice"] = float( re.findall(r'''div.s_mn_house_price2 span::text''', item.extract(), re.DOTALL)[0].strip().replace('￥',''))
                except:
                    pass
            else:
                try:
                    fields["mwprice"] = float( self.remove_html(item.css('div.s_mn_house_price2 span::text').extract_first()).replace('￥',''))
                except:
                    pass

            try:
                fields["commentnum"] = int( item.xpath('''.//*[contains(text(), '评论数')]/preceding-sibling::span[1]/text()''').extract()[0].strip())
            except:
                pass
            try:
                fields["recommendnum"] = int( item.xpath('''.//*[contains(text(), '推荐数')]/preceding-sibling::span[1]/text()''').extract()[0].strip())
            except:
                pass
            try:
                fields["picnum"] = int( item.xpath('''.//*[contains(text(), '房间图片')]/preceding-sibling::span[1]/text()''').extract()[0].strip())
            except:
                pass
            if '(.*?)' in '''div.tmc span::text''':
                try:
                    fields["score"] = float( re.findall(r'''div.tmc span::text''', item.extract(), re.DOTALL)[0].strip())
                except:
                    pass
            else:
                try:
                    fields["score"] = float( self.remove_html(item.css('div.tmc span::text').extract_first()))
                except:
                    pass

            if '(.*?)' in '''test#none''':
                try:
                    fields["riqi"] = str('2025-02-21'+ re.findall(r'''test#none''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["riqi"] = str('2025-02-21'+ self.remove_html(item.css('''test#none''').extract_first()))

                except:
                    pass

            if '(.*?)' in '''a.s_mn_house_t1::attr(href)''':
                try:
                    fields["detailurl"] = str('https://www.muniao.com'+ re.findall(r'''a.s_mn_house_t1::attr(href)''', item.extract(), re.DOTALL)[0].strip())

                except:
                    pass
            else:
                try:
                    fields["detailurl"] = str('https://www.muniao.com'+ self.remove_html(item.css('''a.s_mn_house_t1::attr(href)''').extract_first()))

                except:
                    pass

            yield fields


    # 数据清洗
    def pandas_filter(self):
        engine = create_engine('mysql+pymysql://root:123456@localhost/spiderh45be198?charset=UTF8MB4')
        df = pd.read_sql('select * from mingsuxinxi limit 50', con = engine)

        # 重复数据过滤
        df.duplicated()
        df.drop_duplicates()

        #空数据过滤
        df.isnull()
        df.dropna()

        # 填充空数据
        df.fillna(value = '暂无')

        # 异常值过滤

        # 滤出 大于800 和 小于 100 的
        a = np.random.randint(0, 1000, size = 200)
        cond = (a<=800) & (a>=100)
        a[cond]

        # 过滤正态分布的异常值
        b = np.random.randn(100000)
        # 3σ过滤异常值，σ即是标准差
        cond = np.abs(b) > 3 * 1
        b[cond]

        # 正态分布数据
        df2 = pd.DataFrame(data = np.random.randn(10000,3))
        # 3σ过滤异常值，σ即是标准差
        cond = (df2 > 3*df2.std()).any(axis = 1)
        # 不满⾜条件的⾏索引
        index = df2[cond].index
        # 根据⾏索引，进⾏数据删除
        df2.drop(labels=index,axis = 0)

    # 去除多余html标签
    def remove_html(self, html):
        if html == None:
            return ''
        pattern = re.compile(r'<[^>]+>', re.S)
        return pattern.sub('', html).strip()

    # 数据库连接
    def db_connect(self):
        type = self.settings.get('TYPE', 'mysql')
        host = self.settings.get('HOST', 'localhost')
        port = int(self.settings.get('PORT', 3306))
        user = self.settings.get('USER', 'root')
        password = self.settings.get('PASSWORD', '123456')

        try:
            database = self.databaseName
        except:
            database = self.settings.get('DATABASE', '')

        if type == 'mysql':
            connect = pymysql.connect(host=host, port=port, db=database, user=user, passwd=password, charset='utf8mb4')
        else:
            connect = pymssql.connect(host=host, user=user, password=password, database=database)
        return connect

    # 断表是否存在
    def table_exists(self, cursor, table_name):
        cursor.execute("show tables;")
        tables = [cursor.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]

        if table_name in table_list:
            return 1
        else:
            return 0

    # 数据缓存源
    def temp_data(self):

        connect = self.db_connect()
        cursor = connect.cursor()
        sql = '''
            insert into `mingsuxinxi`(
                id
                ,title
                ,imgurl
                ,huxing
                ,chuzutype
                ,yizhu
                ,address
                ,mwprice
                ,commentnum
                ,recommendnum
                ,picnum
                ,score
                ,riqi
                ,detailurl
            )
            select
                id
                ,title
                ,imgurl
                ,huxing
                ,chuzutype
                ,yizhu
                ,address
                ,mwprice
                ,commentnum
                ,recommendnum
                ,picnum
                ,score
                ,riqi
                ,detailurl
            from `h45be198_mingsuxinxi`
            where(not exists (select
                id
                ,title
                ,imgurl
                ,huxing
                ,chuzutype
                ,yizhu
                ,address
                ,mwprice
                ,commentnum
                ,recommendnum
                ,picnum
                ,score
                ,riqi
                ,detailurl
            from `mingsuxinxi` where
                `mingsuxinxi`.id=`h45be198_mingsuxinxi`.id
            ))
            order by rand()
            limit 50;
        '''

        cursor.execute(sql)
        connect.commit()
        connect.close()
