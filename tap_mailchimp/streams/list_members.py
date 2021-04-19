from datetime import datetime
from tap_mailchimp.streams.base import BaseStream
from tap_mailchimp.state import save_state, incorporate
import singer

LOGGER = singer.get_logger()


class ListMembersStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "list_members"
    KEY_PROPERTIES = ["id"]
    count = 1000
    response_key = "members"

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "exclude_fields": "members._links,members.merge_fields,members.interests,members.marketing_permissions,"
                              "members.location,members.ip_opt,members.ip_signup,members.email_address",
            "since_last_changed": start_date,
            "sort_field": "last_changed",
            "sort_dir": "ASC"
        }
        return params

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))

        # get all lists
        response = self.client.make_request(path='/lists', method='GET', params={"count": 500})
        data = response['lists']
        lists = list(map(lambda x: x['id'], data))

        for list_id in lists:
            self.path = '/lists/{}/members'.format(list_id)
            self.sync_paginated(self.path, False)

        self.state = incorporate(self.state, table, 'last_record', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        save_state(self.state)
