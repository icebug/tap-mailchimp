from datetime import datetime
from tap_mailchimp.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class ListsStream(BaseStream):
    API_METHOD = "GET"
    TABLE = "lists"
    KEY_PROPERTIES = ["id"]
    count = 50
    path = "/lists"
    response_key = "lists"

    def get_params(self, offset, start_date):
        params = {
            "count": self.count,
            "offset": offset,
            "since_date_created": start_date,
            "sort_field": "date_created",
            "sort_dir": "ASC"
        }
        return params

    def get_last_record_date(self, data):
        if len(data) == 0:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return data[-1]['date_created']
