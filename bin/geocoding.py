#!/usr/bin/env python
# coding=utf-8
#
# Copyright © 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals
import app
from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators

from concurrent.futures import ThreadPoolExecutor
import sys
import time
import json
import logging
import logging.handlers
import requests
import re

import geocoding_creds

URL_BASE = "https://maps.googleapis.com/maps/api/geocode/json"
URL_PARAMS = {
    "key": geocoding_creds.api_key,
}

LOG_ROTATION_LOCATION = "/opt/splunk/var/log/splunk/geocoding_command.log"
LOG_ROTATION_BYTES = 1 * 1024 * 1024
LOG_ROTATION_LIMIT = 5

logger = logging.getLogger("geocoding")
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(LOG_ROTATION_LOCATION, maxBytes=LOG_ROTATION_BYTES, backupCount=LOG_ROTATION_LIMIT)
handler.setFormatter(logging.Formatter("[%(levelname)s] (%(threadName)-10s) %(message)s"))
logger.addHandler(handler)


@Configuration()
class geocodingCommand(StreamingCommand):
    threads = Option(require=False, default=8, validate=validators.Integer())

    def stream(self, records):
        pool = ThreadPoolExecutor(self.threads)

        def geocoding_query(record):
            output_fields = [
                "json",
                "time_ms",
                "msg",
                "lat",
                "lon",
                "formatted_address",
                "street_number",
                "route",
                "locality",
                "administrative_area_level_1",
                "administrative_area_level_2",
                "country",
                "postal_code",
            ]

            for key in self.fieldnames:
                # You have to set all possible output fields to ""
                # otherwise if the first row doesn't set the fields
                # then the rest of the rows can't set it.
                for field in output_fields:
                    field = key + "_" + output_field
                    if field in record and not record[field]:
	                record[field] = ""

                params = URL_PARAMS.copy()
                value = record[key].strip()

                if value:
                    params.update({
                        "address": value
                    })

                    try:
                        start = time.time()
                        r = requests.get(URL_BASE, params=params)
                        record[key + "_time_ms"] = (time.time() - start) * 1000

                        r.raise_for_status()
                        r_json = r.json()
                        status = r_json["status"]
                        record[key + "_json"] = r.text
                        record[key + "_msg"] = status

                        logger.info("url: " + r.url)
                        logger.debug("response: " + r.text)
                        logger.debug("status: " + status)

                        if status == "OK":
                            logger.debug("result count: " + str(len(r_json["results"])))
                            result = r_json["results"][0]

                            record[key + "_lat"] = result["geometry"]["location"]["lat"]
                            record[key + "_lon"] = result["geometry"]["location"]["lng"]
                            record[key + "_formatted_address"] = result["formatted_address"]

                            for comp in result["address_components"]:
                                name = comp["types"][0]
                                record[key + "_" + name] = comp["long_name"]

                    except requests.exceptions.HTTPError as err:
                        record[key + "_msg"] = err
                        pass
                    except requests.exceptions.RequestException as e:
                        record[key + "_msg"] = e
                        pass

            return record

        def thread(records):
            chunk = []
            for record in records:
                chunk.append(pool.submit(geocoding_query, record))

                if len(chunk) >= self.threads:
                    yield chunk
                    chunk = []

            if chunk:
                yield chunk

        def unchunk(chunk_gen):
            """Flattens a generator of Future chunks into a generator of Future results."""
            for chunk in chunk_gen:
                for f in chunk:
                    yield f.result() # get result from Future

        # Now iterate over all results in same order as records
        for result in unchunk(thread(records)):
            yield result

dispatch(geocodingCommand, sys.argv, sys.stdin, sys.stdout, __name__)
