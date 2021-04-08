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

        # get all campaign_ids
        campaign_params = {
            "count": 1000,
            "since_send_time": (parse(self.config.get('start_date'))).isoformat(),
            "sort_field": "send_time",
            "sort_dir": "ASC"
        }
        response = self.client.make_request(path='/campaigns', method='GET', params=campaign_params)
        data = response['campaigns']
        campaign_ids = list(map(lambda x: x['id'], data))

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
            record = self.transform_record(record)
            record['report_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for activity in record.get('activity', []):
                new_activity = dict(record)
                del new_activity['activity']
                for key, value in activity.items():
                    new_activity[key] = value
                    transformed.append(new_activity)

        return transformed
