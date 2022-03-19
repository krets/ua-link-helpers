""" Connect to telegram and find links from recent chats.

TELE_API_ID and TELE_API_HASH must be set in environment to run.

Output telelinks.tsv file to be loaded into google sheets for further processing.
  <Channel_Name> <link> <date> <found_on_existing_sheets>

Example Summary Columns:
  Unique Links =UNIQUE(SORT('link dump'!B2:B))
  Occurances      =COUNTIF('link dump'!B:B,A2)
  First Seen      =MINIFS('link dump'!C:C,'link dump'!B:B,A2)
  Last Seen       =MAXIFS('link dump'!C:C,'link dump'!B:B,A2)
  Existing Sheets =VLOOKUP(A2, 'link dump'!B:D, 3, FALSE)

"""
import argparse
import json
import logging
import os
import re
from collections import defaultdict
from datetime import timedelta, datetime

import pytz
from telethon.sync import TelegramClient

import linkcheck

LOG = logging.getLogger('ualinks')
TELE_API_ID = os.environ.get('TELE_API_ID')
TELE_API_HASH = os.environ.get('TELE_API_HASH')

TELE_CHANNELS = 'telechannels.json'
URL_RE = re.compile("(https?://[^\s]+)")
DATE_FMT = "%Y-%m-%d %H:%M:%S"
MIN_AGE = 36
TSV_OUTPUT = "telelinks.tsv"


def main():
    args = parse_args()
    level = logging.WARN
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    LOG.setLevel(level)

    if not all([TELE_API_ID, TELE_API_HASH]):
        LOG.warning("TELE_API_ID or TELE_API_HASH not set correctly in environment.")

    with open(TELE_CHANNELS, 'r') as fh:
        channels = json.load(fh)

    client = TelegramClient('ualinks', TELE_API_ID, TELE_API_HASH)
    client.start()
    now = datetime.now()

    threshhold = pytz.UTC.localize(now - timedelta(hours=args.hours))

    links = defaultdict(list)
    for channel_name, channel_id in channels.items():
        LOG.info("Looking up %s (%s)", channel_name, channel_id)
        oldest = None
        newest = None
        kwargs = {'limit': 200}
        try:
            for _ in range(99):
                LOG.debug("Iter (%d) on %s", _, channel_name)
                for message in client.get_messages(channel_id, **kwargs):
                    date_string = message.date.strftime(DATE_FMT)
                    if oldest is None or message.date < oldest.date:
                        oldest = message
                    if newest is None or message.date > newest.date:
                        newest = message

                    for url in URL_RE.findall(str(message.message)):
                        links[channel_name].append((url, date_string))
                if oldest.date < threshhold:
                    LOG.debug("Oldest message exceeds window")
                    LOG.info("Channel complete")
                    break
                else:
                    LOG.info("Going deeper. max_id=%s", oldest.id)
                    kwargs['max_id'] = oldest.id
        except ValueError:
            LOG.warning("Can't connect to: %s" % channel_name)
            pass

    existing_links = {}
    if args.compare:
        existing_links = linkcheck.get_links(args.compare)

    with open(args.output, 'w') as fh:
        for channel_name, items in links.items():
            for url, timestamp in items:
                found_sheet = []
                for sheet, links in existing_links.items():
                    if url in links:
                        found_sheet.append(sheet)

                line = "\t".join([str(channel_name), url, timestamp, ','.join(found_sheet)])
                fh.write(line+"\n")
                print(line)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=str, default=TSV_OUTPUT,
                        help='Path where output TSV will be written (default: %s)' % TSV_OUTPUT)
    parser.add_argument('-c', '--compare', type=str, help="Excel document to compare")
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show info messages.')
    parser.add_argument('--hours', type=int, default=MIN_AGE,
                        help='Number of hours to collect (default: %s)' % MIN_AGE)
    return parser.parse_args()

if __name__=='__main__':
    main()