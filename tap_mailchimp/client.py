import time
import requests
import singer

from tap_framework.client import BaseClient


LOGGER = singer.get_logger()


class MailchimpClient(BaseClient):
    def __init__(self, config):
        super().__init__(config)
        self.__user_agent = config.get('user_agent')
        self.__api_key = config.get('api_key')
        self.__session = requests.Session()
        self.__base_url = None
        self.page_size = int(config.get('page_size', '500'))
        self.__base_url = 'https://{}.api.mailchimp.com/3.0'.format(config.get('dc'))

    def get_headers(self):
        headers = {
            "Content-Type": "application/json",
            "Authorization": "OAuth {}".format(self.__api_key)
        }
        if self.__user_agent:
            headers["User-Agent"] = self.__user_agent
        return headers

    def make_request(self, path, method, base_backoff=45, params=None, body=None):
        headers = self.get_headers()

        LOGGER.info("Making {} request to {}".format(method, path))
        url = self.__base_url + path

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=body)

        if response.status_code == 429:
            LOGGER.info('Got a 429, sleeping for {} seconds and trying again'
                        .format(base_backoff))
            time.sleep(base_backoff)
            return self.make_request(path, method, base_backoff * 2, params, body)

        if response.status_code != 200:
            raise RuntimeError(response.text)
        return response.json()

    def make_aws_request(self, method, url):
        LOGGER.info("Making {} request to {}".format(method, url))

        response = requests.request(
            method=method,
            url=url,
            stream=True)

        return response
