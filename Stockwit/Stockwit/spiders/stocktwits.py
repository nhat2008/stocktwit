__author__ = 'nhat'
import json
import urllib2
from pymongo import MongoClient, ReadPreference
import json
import HTMLParser
from time import sleep


if __name__ == "__main__":

    print urllib2.urlopen('http://stocktwits.com/symbol/CXR.CA')

    MAIN_DB_HOST1 = 'localhost'
    MAIN_DB_PORT = 27017
    REL_DB = 'Stocktwit'
    REL_COLL = 'Trend'

    client = MongoClient(MAIN_DB_HOST1, MAIN_DB_PORT)
    read_preference = ReadPreference.SECONDARY
    rel_coll = client[REL_DB][REL_COLL]
    html_parser = HTMLParser.HTMLParser()
    key = ['8fa29e7a7c8b8651fad8077bb4675e6798a85618', '3b66ed021ad3536f7728c81834b64fdf3c3a20c4']

    url = ['https://api.stocktwits.com/api/2/streams/trending.json?access_token=',
           'https://api.stocktwits.com/api/2/streams/suggested.json?access_token=',
           'https://api.stocktwits.com/api/2/streams/charts.json?access_token=']

    index = 0
    flag_run = 1
    while (True):

        sleep(10)
        full_url = url[flag_run % 3] + key[index]
        flag_run = flag_run + 1
        if flag_run > 100:
            flag_run = 0
        print full_url
        jsonresponse = json.load(urllib2.urlopen(full_url))

        # print jsonresponse
        respone_status = jsonresponse['response']['status']
        if respone_status == 200:
            try:
                messages = jsonresponse["messages"]
                for message in messages:

                    body = html_parser.unescape(message['body'])
                    user_name = message['user']['username']
                    print "User", user_name
                    time = message['created_at']
                    all_symbols = ''
                    symbols = message.get('symbols', False) or ''
                    for symbol in symbols:
                        all_symbols = all_symbols + symbol['symbol'] + '(' + symbol['title'] + ')' + ';'

                    if rel_coll.find({'user_name': user_name, "time": time}).count() == 0:
                        rel_coll.insert({"user_name": user_name, "body": body,
                                         'time': time,
                                         'all_symbols': all_symbols})

            except ValueError:
                print ValueError
        else:
            if respone_status == 429:
                index = index + 1
                if index == len(key) - 1:
                    index = 0
