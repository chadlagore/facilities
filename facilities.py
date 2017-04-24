'''
Basic spider to collect raw json data on facilities, converts to CSV.
See `output.csv'.

@author Chad Lagore

'''

import json
import csv

import scrapy
from scrapy.crawler import CrawlerProcess


with open('ids.json') as infile:
    ids = list(json.load(infile))


def first(iterator, condition=lambda x: True, default=None):
    '''
    Returns the first element in the iterator or `default`
    '''

    return next((x for x in iterator if condition(x)), default)


class FacilitiesSpider(scrapy.Spider):

    base_url = 'https://opendata.epa.gov/data/facility/'
    name = 'facilities_spider'

    def start_requests(self):
        '''
        Creates a row for facility id.

        '''

        for i in ids:
            yield scrapy.Request(
                url=self.base_url + i + '.json',
                callback=self.parse,
                meta={'id': i}
            )

    def parse(self, response):
        '''
        Parses JSON.

        '''

        payload = json.loads(response.text)
        record = {
            'id': response.meta['id']
        }

        for element in payload:
            for key, val in element.items():
                column = key.split('#')[-1]
                if '/' in column:
                    column = key.split('/')[-1]
                data = first(val)
                if isinstance(data, dict):
                    record[column] = first(data.values())

        self.to_csv(record)

    def to_csv(self, record):
        '''
        Appends a line to the output csv.

        '''

        with open('output.csv', 'r+b') as outfile:
            cols = next(csv.reader(outfile))
            record = {
                key: value for key, value in record.items() if key in cols
            }
            writer = csv.DictWriter(outfile, cols, None)
            writer.writerow(record)


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(FacilitiesSpider)
process.start()
