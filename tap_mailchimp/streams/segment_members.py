from datetime import datetime
from tap_mailchimp.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()

# No support in API for state


class SegmentsMemberStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "segment_members"
    KEY_PROPERTIES = ["id"]
    count = 1000
    response_key = "members"
    segment_id = ''

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))

        # get all lists
        response = self.client.make_request(path='/lists', method='GET', params={"count": 500})
        data = response['lists']
        lists = list(map(lambda x: x['id'], data))

        for list_id in lists:
            path = '/lists/{}/segments'.format(list_id)
            response = self.client.make_request(path=path, method='GET', params={"count": 500})
            data = response['segments']
            segments = list(map(lambda x: x['id'], data))

            for segment_id in segments:
                self.segment_id = segment_id
                self.path = '/lists/{0}/segments/{1}/members'.format(list_id, segment_id)
                self.sync_paginated(self.path, False)

    def get_stream_data(self, response):
        transformed = []

        for record in response[self.response_key]:
            record = self.transform_record(record)
            record['reportDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record['segment_id'] = self.segment_id
            transformed.append(record)

        return transformed
