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

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "exclude_fields": "members._links,members.merge_fields,members.interests"
        }
        return params

    def sync_data(self):
        LOGGER.info("Syncing data for {}".format(self.TABLE))

        # get all lists
        response = self.client.make_request(path='/lists', method='GET', params={"count": 1000})
        data = response['lists']
        lists = list(map(lambda x: x['id'], data))

        operations = []
        for list_id in lists:
            path = '/lists/{}/segments'.format(list_id)
            # get all segments
            response = self.client.make_request(path=path, method='GET', params={"count": 1000})
            data = response['segments']
            segments = list(map(lambda x: str(x['id']), data))

            for segment_id in segments:
                operations.append(
                    {
                        'method': self.API_METHOD,
                        'path': '/lists/{0}/segments/{1}/members'.format(list_id, segment_id),
                        'operation_id': segment_id,
                        'params': {
                            'since': self.get_start_date(self.TABLE).isoformat(),
                            'exclude_fields': "members._links,members.merge_fields,members.interests"
                        }
                    }
                )

        self.batch_sync_data(operations)

    def get_stream_data(self, response, operation_id=None):
        transformed = []

        for record in response[self.response_key]:
            record = self.transform_record(record)
            record['reportDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record['segment_id'] = str(operation_id).split('-')[0]
            transformed.append(record)

        return transformed
