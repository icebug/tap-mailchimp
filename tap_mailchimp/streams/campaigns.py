from datetime import datetime
from tap_mailchimp.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class CampaignsStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "campaigns"
    KEY_PROPERTIES = ["id"]
    count = 500
    path = "/campaigns"
    response_key = "campaigns"

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "exclude_fields": 'campaigns._links',
            "status": "sent",
            "since_send_time": start_date,
            "sort_field": "send_time",
            "sort_dir": "ASC"
        }
        return params

    def get_last_record_date(self, data):
        if len(data) == 0:
            return datetime.now().isoformat()
        return data[-1]['send_time']
