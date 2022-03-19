""" Load excel file downloaded from master google doc and perform a GET on every link.
"""
import argparse
import logging
import re
from multiprocessing.dummy import Pool
from collections import defaultdict

import openpyxl
import requests
import requests_cache
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import ConnectionError

LOG = logging.getLogger('ualinks')

_HEADERS = {
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "en-GB,en;q=0.9"
}

def get_links(xls):
    """Load the excel file and search the first two columns for links.

    Returns (defaultdict) {'sheetName': [url,...], ...}
    """
    LOG.info("Searching file for links: %s", xls)
    url_re = re.compile("(https?://[^\s]+)")
    links = defaultdict(list)
    workbook = openpyxl.load_workbook(xls)
    for name in workbook.sheetnames:
        sheet = workbook[name]
        for row in sheet:
            values = row[0].value, row[1].value
            if not any(values):
                continue
            for val in values:
                if not val:
                    continue
                match = url_re.search(str(val))
                if match:
                    links[name].append(match.groups()[0])
    return links

def link_check(item):
    sheet, url = item
    LOG.debug("Getting (%s) %s", sheet, url)

    try:
        resp = requests.get(url, allow_redirects=True, headers=_HEADERS, verify=False)
    except ConnectionError as error:
        LOG.warning("Error connecting to %s: %s", url, error)
        return sheet, '0', url, "", "ConnectionError", ""

    try:
        title = str(resp.content).split("<title>")[-1].split("title>")[0][:-2][:100]
    except:
        title = "?"

    redirect = ""
    if url != resp.url:
        redirect = resp.url

    return sheet, str(resp.status_code), url, redirect, title, resp.headers.get('Server', '')


def test_links(all_links):
    pool = Pool()
    rows = ["Sheet", "Status", "Link", "Redirect", "Title", 'Server']
    work = []
    for sheet, links in all_links.items():
        for url in links:
            work.append((sheet, url))

    report = pool.map(link_check, work)
    return [rows]+report


def main():
    args = parse_args()
    level = logging.WARN
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    LOG.setLevel(level)
    report = test_links(get_links(args.xls))
    for row in report:
        print("\t".join(row))
    pass

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('xls', help="Excel document to read")
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug messages.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show info messages.')
    return parser.parse_args()

if __name__ == '__main__':
    requests_cache.install_cache()
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    main()