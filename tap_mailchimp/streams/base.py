from datetime import datetime
import json
import time
import tarfile

import singer
import singer.utils
import singer.metrics
from dateutil.parser import parse

from tap_mailchimp.state import incorporate, save_state, get_last_record_value_for_table
from tap_framework.streams import BaseStream as base
from tap_mailchimp.cache import stream_cache

from requests.exceptions import HTTPError

LOGGER = singer.get_logger()


class BaseStream(base):
    MAX_RETRY_ELAPSED_TIME = 43200  # 12 hours
    KEY_PROPERTIES = ["id"]
    CACHE = False
    TABLE = ''
    count = 1000
    path = '/'
    response_key = ''

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset
        }
        return params

    def get_start_date(self, table):
        start_date = get_last_record_value_for_table(self.state, table)
        if start_date is None:
            start_date = parse(self.config.get('start_date'))
        LOGGER.info('Syncing data from {}'.format(start_date.isoformat()))
        return start_date

    def sync_paginated(self, path, should_save_state):
        table = self.TABLE
        total_items = 100
        offset = 0

        while offset < total_items:
            start_date = self.get_start_date(table)
            params = self.get_params(offset, start_date.isoformat())
            response = self.client.make_request(path=path, method=self.API_METHOD, params=params)
            transformed = self.get_stream_data(response)

            with singer.metrics.record_counter(endpoint=table) as counter:
                singer.write_records(table, transformed)
                counter.increment(len(transformed))

            if self.CACHE:
                stream_cache[table].extend(transformed)

            total_items = response.get("total_items", 0)
            offset += self.count

            if should_save_state:
                data = response.get(self.response_key, [])
                self.state = incorporate(self.state, table, 'last_record', self.get_last_record_date(data))
                save_state(self.state)

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))
        self.sync_paginated(self.path, True)

        return self.state

    def get_stream_data(self, response, operation_id=None):
        transformed = []

        for record in response[self.response_key]:
            record = self.transform_record(record)
            record['report_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            transformed.append(record)

        return transformed

    def get_last_record_date(self, data):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def batch_sync_data(self, operations):
        response = self.client.make_request(path='/batches', method='POST', body=json.dumps({'operations': operations}))
        batch_id = response['id']
        LOGGER.info('%s - Job running: %s', self.TABLE, batch_id)

        data = self.poll_batch_status(batch_id)
        LOGGER.info('%s - Batch job complete: took %.2fs minutes', self.TABLE,
                    (singer.utils.strptime_to_utc(data['completed_at']) -
                     singer.utils.strptime_to_utc(data['submitted_at']))
                    .total_seconds() / 60)

        failed_ids = self.save_batch_data(data['response_body_url'])
        if failed_ids:
            LOGGER.warning("{} - operations failed for ids: {}".format(self.TABLE, failed_ids))

        self.state = incorporate(self.state, self.TABLE, 'last_record', self.get_last_record_date(data))
        save_state(self.state)

    def poll_batch_status(self, batch_id):
        sleep = 120
        start_time = time.time()
        while True:
            data = self.get_batch_info(batch_id)

            progress = ''
            if data['total_operations'] > 0:
                progress = ' ({}/{} {:.2f}%)'.format(
                    data['finished_operations'],
                    data['total_operations'],
                    (data['finished_operations'] / data['total_operations']) * 100.0)

            LOGGER.info('%s - Job polling: %s - %s%s', self.TABLE, data['id'], data['status'], progress)

            if data['status'] == 'finished':
                return data
            elif (time.time() - start_time) > self.MAX_RETRY_ELAPSED_TIME:
                message = '{} - export deadline exceeded ({} secs)'.format(self.TABLE, self.MAX_RETRY_ELAPSED_TIME)
                LOGGER.error(message)
                raise Exception(message)

            LOGGER.info('%s - status: %s, sleeping for %s seconds', self.TABLE, data['status'], sleep)
            time.sleep(sleep)

    def get_batch_info(self, batch_id):
        try:
            return self.client.make_request(path='/batches/{}'.format(batch_id), method='GET')
        except HTTPError as e:
            raise e

    def save_batch_data(self, response_body_url):
        failed_ids = []
        with self.client.make_aws_request(method='GET', url=response_body_url) as response:
            with tarfile.open(mode='r|gz', fileobj=response.raw) as tar:
                file = tar.next()
                while file:
                    if file.isfile():
                        raw_operations = tar.extractfile(file)
                        operations = json.loads(raw_operations.read().decode('utf-8'))

                        for i, operation in enumerate(operations):
                            operation_id = operation['operation_id']
                            LOGGER.info("%s - [batch operation %s] Processing records for id %s",
                                        self.TABLE, i, operation_id)

                            if operation['status_code'] != 200:
                                failed_ids.append(operation_id)
                            else:
                                response = json.loads(operation['response'])
                                transformed = self.get_stream_data(response, operation_id)
                                with singer.metrics.record_counter(endpoint=self.TABLE) as counter:
                                    singer.write_records(self.TABLE, transformed)
                                    counter.increment(len(transformed))

                    file = tar.next()
        return failed_ids
