# The MIT License (MIT)
#
# Copyright (c) 2015 dallasdata
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Reference for Socrata SODA API:
#
#   http://dev.socrata.com/docs/endpoints.html

'''
Utilities for interacting with Socrata datasets.
'''

import datetime
import io
import json
import logging
import urllib.request

__LOGGER = logging.getLogger(__name__)

__PAGESZ = 20000

def floating_timestamp_to_datetime(ts, tz=None):
    '''
    Return a datetime.datetime object for the given Socrata floating timestamp.

    http://dev.socrata.com/docs/datatypes/timestamp.html
    '''

    d = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
    if tz:
        d = tz.localize(d)

    return d


# It would be nice to load this via CSV rather than JSON, as the former lends
# itself to easier incremental parsing via included libraries. JSON can be
# incrementally parsed via ijson, but it's not available via MacPorts so punt
# for now. We choose JSON here because it allows access to internal Socrata
# fields which CSV does not.
def dataset_rows(api_host, dataset_id, system_fields=False):
    '''
    Iterator for rows in the given dataset. Each row is a Python dictionary.
    '''

    offset = 0
    while True:
        __LOGGER.debug('Fetching rows [{}, {}) from {}'.format(
                offset, offset + __PAGESZ, dataset_id))
        url = ('http://{host}/resource/{dataset_id}.json?'
               '$offset={off}&$limit={lim}&$$exclude_system_fields={sys}').format(
                host=api_host, dataset_id=dataset_id, off=offset, lim=__PAGESZ,
                sys=not system_fields)
        rows = json.load(io.TextIOWrapper(urllib.request.urlopen(url),
                                          encoding='utf-8',
                                          errors='replace'))
        for r in rows:
            yield r

        if len(rows) < __PAGESZ:
            break

        offset += len(rows)
