#!/usr/bin/env python3

import singer

import tap_framework

from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.streams import AVAILABLE_STREAMS

LOGGER = singer.get_logger()  # noqa


class MailchimpRunner(tap_framework.Runner):
    pass


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(required_config_keys=["api_key", "dc", "start_date"])
    client = MailchimpClient(args.config)
    runner = MailchimpRunner(args, client, AVAILABLE_STREAMS)

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == "__main__":
    main()
