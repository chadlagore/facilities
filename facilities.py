'''
Basic spider to collect raw json data on facilities, converts to CSV.
See `output.csv'.

@author Chad Lagore

'''

import csv
import json
import time

import requests


with open('ids.json') as infile:
    ids = list(json.load(infile))


def first(iterator, condition=lambda x: True, default=None):
    '''
    Returns the first element in the iterator or `default`
    '''

    return next((x for x in iterator if condition(x)), default)


class FacilitiesSpider:

    base_url = 'https://opendata.epa.gov/data/facility/'
    name = 'facilities_spider'
    throttle = 3 # Seconds between requests.

    def start_requests(self):
        '''
        Creates a row for facility id.

        '''

        # Yield each url to parse function.
        for i in ids:
            url = self.base_url + i + '.json'
            print("Requesting " + url)
            self.parse(requests.get(url), i)
            time.sleep(self.throttle)


    def parse(self, response, id):
        '''
        Parses JSON.

        '''

        # Collect JSON payload, generate new record.
        payload = json.loads(response.text)
        record = {
            'id': id
        }

        # Parse JSON payload.
        for element in payload:
            for key, val in element.items():

                # Columns names are in keys, need to manipulate.
                column = key.split('#')[-1]
                if '/' in column:
                    column = key.split('/')[-1]
                data = first(val)

                # Collect dictionary looking rows.
                if isinstance(data, dict):
                    record[column] = first(data.values())

        # Send to CSV.
        self.to_csv(record)

    def to_csv(self, record):
        '''
        Appends a line to the output csv.

        '''

        # Write new rows to csv.
        with open('output.csv', 'r') as rfile:
            cols = next(csv.reader(rfile))

            # Keep recognizable rows.
            record = {
                key: value for key, value in record.items() if key in cols
            }

            with open('output.csv', 'a') as afile:
                writer = csv.DictWriter(afile, cols, None)
                writer.writerow(record)

# Start crawling.
spider = FacilitiesSpider()
spider.start_requests()
