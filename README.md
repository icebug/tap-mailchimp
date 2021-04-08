# tap-mailchimp

Author: Maria Larsson (maria.larsson@smartr.se)

This is a [Singer](http://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It:
- Generates a catalog of available data in Mailchimp
- Extracts the following resources:

  - [lists](https://mailchimp.com/developer/marketing/api/lists/get-lists-info/)
  - [list_members](https://mailchimp.com/developer/marketing/api/list-members/list-members-info/)
  - [segments](https://mailchimp.com/developer/marketing/api/list-segments/list-segments/)
  - [segment_members](https://mailchimp.com/developer/marketing/api/list-segment-members/list-members-in-segment/)
  - [campaigns](https://mailchimp.com/developer/marketing/api/campaigns/list-campaigns/)
  - [reports_email_activity](https://mailchimp.com/developer/marketing/api/email-activity-reports/list-email-activity/)


### Quick Start

1. Install

```bash
git clone git@github.com:icebug/tap-mailchimp.git
cd tap-mailchimp
pip install -e .
```

2. Get an API key from Mailchimp
<!---
TODO: include a helpful URL
--->


3. Create the config file.

There is a template you can use at `config.json.example`, just copy it to `config.json` in the repo root and insert your token and email

4. Run the application to generate a catalog.

```bash
$ tap-mailchimp -c config.json --discover > catalog.json
```

Note: if you want to automatically select all fields, run:
```
$ tap-mailchimp -c config.json --discover | jq '.streams[].metadata[0].metadata.selected = true' > catalog.json
```
(Check out the included Makefile for shortcuts)

5. Select the tables you'd like to replicate

Step 4 generates a a file called `catalog.json` that specifies all the available endpoints and fields. You'll need to open the file and select the ones you'd like to replicate. See the [Singer guide on Catalog Format](https://github.com/singer-io/getting-started/blob/c3de2a10e10164689ddd6f24fee7289184682c1f/BEST_PRACTICES.md#catalog-format) for more information on how tables are selected.

6. Run it!

```bash
tap-mailchimp -c config.json --catalog catalog.json
```

Copyright &copy; 2021 Icebug
