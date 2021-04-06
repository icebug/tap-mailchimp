from tap_mailchimp.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class ReportsEmailActivityStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "reports_email_activity"
    KEY_PROPERTIES = ["campaign_id", "list_id", "email_id", "timestamp"]
    count = 1000
    response_key = "emails"

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "since": start_date
        }
        return params

    def sync_data(self):
        table = self.TABLE
        LOGGER.info("Syncing data for {}".format(table))

        # get all campaigns
        response = self.client.make_request(path='/campaigns', method='GET', params={"count": 500})
        data = response['campaigns']
        campaigns = list(map(lambda x: x['id'], data))

        for campaign_id in campaigns:
            self.path = '/reports/{}/email-activity'.format(campaign_id)
            self.sync_paginated(self.path)