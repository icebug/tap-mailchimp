from tap_mailchimp.streams.base import BaseStream
import singer
from datetime import datetime
from dateutil.parser import parse


LOGGER = singer.get_logger()


class ReportsEmailActivityStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "reports_email_activity"
    response_key = "emails"

    def sync_data(self):
        LOGGER.info("Syncing data for {}".format(self.TABLE))
        total_campaigns = 100
        count = 1000
        offset = 0

        campaign_ids = []
        while offset < total_campaigns:
            campaign_params = {
                "count": count,
                "offset": offset,
                "since_send_time": (parse(self.config.get('start_date'))).isoformat(),
                "sort_field": "send_time",
                "sort_dir": "ASC"
            }
            response = self.client.make_request(path='/campaigns', method='GET', params=campaign_params)
            total_campaigns = response['total_items']

            data = response['campaigns']
            campaign_ids += list(map(lambda x: x['id'], data))

            offset += count

        LOGGER.info('Number of campaigns: {}'.format(len(campaign_ids)))
        operations = []
        for campaign_id in campaign_ids:
            operations.append(
                {
                    'method': self.API_METHOD,
                    'path': '/reports/{}/email-activity'.format(campaign_id),
                    'operation_id': campaign_id,
                    'params': {
                        'since': self.get_start_date(self.TABLE).isoformat(),
                        'exclude_fields': '_links,emails._links'
                    }
                }
            )
        self.batch_sync_data(operations)

    def get_stream_data(self, response, operation_id=None):
        transformed = []

        for record in response[self.response_key]:
            for activity in record.get('activity', []):
                new_activity = dict(record)
                del new_activity['activity']
                for key, value in activity.items():
                    new_activity[key] = value
                    new_activity = self.transform_record(new_activity)
                    new_activity['report_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    transformed.append(new_activity)

        return transformed
