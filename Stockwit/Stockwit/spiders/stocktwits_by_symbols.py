from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from pymongo import MongoClient, ReadPreference
import pandas as pd
import csv
import json


class MySpider(BaseSpider):
    name = "Stocktwit"
    allowed_domains = ["stocktwits.com"]
    start_urls = []

    url = 'http://stocktwits.com/symbol/'

    MAIN_DB_HOST1 = 'localhost'
    MAIN_DB_PORT = 27017
    REL_DB = 'Stocktwit'
    REL_COLL = 'Crawl'

    client = MongoClient(MAIN_DB_HOST1, MAIN_DB_PORT)
    read_preference = ReadPreference.SECONDARY
    rel_coll = client[REL_DB][REL_COLL]

    # DOWNLOAD_DELAY = 5

    def __init__(self):


        df = pd.read_csv('2014.csv', encoding='utf-8')
        for index, row in df.iterrows():
            #print row['symbol']
            self.start_urls.append(self.url + row['symbol'])

        print '========================================================Added'


    def parse(self, response):

        hxs = HtmlXPathSelector(response)

        datas = hxs.select("//ol[@id='updates']/li/@data-src").extract()

        for data in datas:
            #print data
            new = json.loads(data)
            id = (new.get('id', False) or '')
            body = new.get('body', False) or ''
            user = new['user']['username']
            created_at = new['created_at']
            print created_at

            if self.rel_coll.find({'id': id}).count() == 0:
                self.rel_coll.insert({"id": id, "body": body, "user": user, "created_at": created_at})