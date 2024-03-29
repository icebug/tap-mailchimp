from datetime import datetime
from dateutil.parser import parse
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
            "exclude_fields": "members._links,members.merge_fields,members.interests,"
                              "members.ip_opt,members.ip_signup,members.email_address"
        }
        return params

    def sync_data(self):
        LOGGER.info("Syncing data for {}".format(self.TABLE))
        total_lists = 100
        offset = 0
        count = 100

        # get all lists
        list_ids = []
        while offset < total_lists:
            list_params = {
                "count": count,
                "offset": offset,
                "since_date_created": (parse(self.config.get('start_date'))).isoformat(),
                "sort_field": "date_created",
                "sort_dir": "ASC",
                "fields": "lists.id,total_items"
            }
            response = self.client.make_request(path='/lists', method='GET', params=list_params)
            data = response['lists']
            list_ids += list(map(lambda x: x['id'], data))

            total_lists = response['total_items']
            offset += count

        operations = []
        for list_id in list_ids:
            path = '/lists/{}/segments'.format(list_id)

            # get all segments
            total_segments = 100
            offset = 0
            count = 1000

            segments = {}
            member_count = 0
            while offset < total_segments:
                segment_params = {
                    "count": count,
                    "offset": offset,
                    "fields": "segments.id,total_items,segments.member_count"
                }

                response = self.client.make_request(path=path, method='GET', params=segment_params)
                data = response['segments']
                segment_ids = list(map(lambda x: str(x['id']), data))
                member_count = list(map(lambda x: int(x['member_count']), data))
                new_segments = dict((x, y) for x, y in zip(segment_ids, member_count))
                segments = {**segments, **new_segments}

                total_segments = response['total_items']
                offset += count

            for segment_id, member_count in segments.items():
                offset = 0
                count = 1000

                while offset < member_count:

                    params = {
                        'count': count,
                        'offset': offset,
                        'since': self.get_start_date(self.TABLE).isoformat(),
                        'exclude_fields': "members._links,members.merge_fields,members.interests,members.location"
                    }
                    operations.append(
                        {
                            'method': self.API_METHOD,
                            'path': '/lists/{0}/segments/{1}/members'.format(list_id, segment_id),
                            'operation_id': segment_id,
                            'params': params
                        }
                    )

                    offset += count

        LOGGER.info('Batching {} operations'.format(len(operations)))
        self.batch_sync_data(operations)

    def get_stream_data(self, response, operation_id=None):
        transformed = []

        for record in response[self.response_key]:
            record = self.transform_record(record)
            record['reportDate'] = datetime.now().isoformat()
            record['segment_id'] = str(operation_id).split('-')[0]
            transformed.append(record)

        return transformed
