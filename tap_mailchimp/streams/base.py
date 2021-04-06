from datetime import datetime

import singer
import singer.utils
import singer.metrics
from dateutil.parser import parse

from tap_mailchimp.state import incorporate, save_state, get_last_record_value_for_table
from tap_framework.streams import BaseStream as base
from tap_mailchimp.cache import stream_cache


LOGGER = singer.get_logger()


class BaseStream(base):
    KEY_PROPERTIES = ["id"]
    CACHE = False
    count = 500
    path = '/'
    response_key = ''

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset
        }
        return params

    def sync_paginated(self, path):
        table = self.TABLE
        total_items = 100
        offset = 0

        date = get_last_record_value_for_table(self.state, table)
        if date is None:
            date = parse(self.config.get('start_date'))
        LOGGER.info('Syncing data from {}'.format(date.isoformat()))

        while offset < total_items:
            params = self.get_params(offset, date.isoformat())
            response = self.client.make_request(path=path, method=self.API_METHOD, params=params)
            transformed = self.get_stream_data(response)

            with singer.metrics.record_counter(endpoint=table) as counter:
                singer.write_records(table, transformed)
                counter.increment(len(transformed))

            if self.CACHE:
                stream_cache[table].extend(transformed)

            data = response.get(self.response_key, [])
            last_record_date = self.get_last_record_date(data)
            total_items = response.get("total_items", 0)
            offset += self.count

            self.state = incorporate(self.state, table, 'last_record', last_record_date)
            save_state(self.state)

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))
        self.sync_paginated(self.path)

        return self.state

    def get_stream_data(self, response):
        transformed = []

        for record in response[self.response_key]:
            record = self.transform_record(record)
            record['reportDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            transformed.append(record)

        return transformed

    def get_last_record_date(self, data):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
