from datetime import datetime
from tap_mailchimp.streams.base import BaseStream
from tap_mailchimp.state import save_state, incorporate
import singer

LOGGER = singer.get_logger()


class SegmentsStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "segments"
    KEY_PROPERTIES = ["id"]
    count = 1000
    response_key = "segments"

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "since_updated_at": start_date
        }
        return params

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))

        # get all lists
        response = self.client.make_request(path='/lists', method='GET', params={"count": 500})
        data = response['lists']
        lists = list(map(lambda x: str(x['id']), data))

        for list_id in lists:
            path = '/lists/{}/segments'.format(list_id)
            self.sync_paginated(path, False)

        self.state = incorporate(self.state, table, 'last_record', datetime.now().isoformat())
        save_state(self.state)

        return self.state
