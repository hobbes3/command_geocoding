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

        def geocoding_query(record_dict):
            params = URL_PARAMS.copy()

            record = record_dict["record"]
            input_field = record_dict["field"]
            input_value = record[input_field].strip()

            output_fields = {}
            output_fields[input_field + "_json"] = ""
            output_fields[input_field + "_time_ms"] = ""
            output_fields[input_field + "_msg"] = ""
            output_fields[input_field + "_lat"] = ""
            output_fields[input_field + "_lon"] = ""
            output_fields[input_field + "_formatted_address"] = ""
            output_fields[input_field + "_street_number"] = ""
            output_fields[input_field + "_route"] = ""
            output_fields[input_field + "_locality"] = ""
            output_fields[input_field + "_administrative_area_level_2"] = ""
            output_fields[input_field + "_administrative_area_level_1"] = ""
            output_fields[input_field + "_country"] = ""
            output_fields[input_field + "_postal_code"] = ""

            params.update({
                "address": input_value
            })

            try:
                start = time.time()
                r = requests.get(URL_BASE, params=params)
                output_fields[input_field + "_time_ms"] = (time.time() - start) * 1000

                r.raise_for_status()
                r_json = r.json()
                status = r_json["status"]
                output_fields[input_field + "_json"] = r.text
                output_fields[input_field + "_msg"] = status

                logger.info("url: " + r.url)
                logger.debug("response: " + r.text)
                logger.debug("status: " + status)

                if status == "OK":
                    logger.debug("result count: " + str(len(r_json["results"])))
                    result = r_json["results"][0]

                    output_fields[input_field + "_lat"] = result["geometry"]["location"]["lat"]
                    output_fields[input_field + "_lon"] = result["geometry"]["location"]["lng"]
                    output_fields[input_field + "_formatted_address"] = result["formatted_address"]

                    for comp in result["address_components"]:
                        key = comp["types"][0]
                        output_fields[input_field + "_" + key] = comp["long_name"]

            except requests.exceptions.HTTPError as err:
                output_fields[input_field + "_msg"] = err
                pass
            except requests.exceptions.RequestException as e:
                output_fields[input_field + "_msg"] = e
                pass

            record.update(output_fields)
            return record

        def thread(records):
            chunk = []
            for record in records:
                for field in self.fieldnames:
                    chunk.append(pool.submit(geocoding_query, {"record": record, "field": field}))

                if len(chunk) == self.threads:
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
        for i, result in enumerate(unchunk(thread(records))):
            if (i + 1) % len(self.fieldnames) == 0:
                yield result

dispatch(geocodingCommand, sys.argv, sys.stdin, sys.stdout, __name__)
