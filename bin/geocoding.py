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
import math
import splunk.Intersplunk
import splunklib.client as client
import splunklib.searchcommands as searchcommands
import os
from math import pi 

LOG_ROTATION_LOCATION = os.environ['SPLUNK_HOME'] + "/var/log/splunk/command_geocoding.log"
LOG_ROTATION_BYTES = 1 * 1024 * 1024
LOG_ROTATION_LIMIT = 5

logger = logging.getLogger("geocoding")
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(LOG_ROTATION_LOCATION, maxBytes=LOG_ROTATION_BYTES, backupCount=LOG_ROTATION_LIMIT)
handler.setFormatter(logging.Formatter("[%(levelname)s] (%(threadName)-10s) %(message)s"))
logger.addHandler(handler)

URL_BASE = "https://maps.googleapis.com/maps/api/geocode/json"


@Configuration()
class geocodingCommand(StreamingCommand):
    threads = Option(require=False, default=8, validate=validators.Integer())
    null_value = Option(require=False, default="")
    unit = Option(require=False, default="mi")

    def stream(self, records):
        service = self.service
        storage_passwords = service.storage_passwords

        for credential in storage_passwords:
            if credential.content.get('realm') == "command_geocoding":
                self.APIKey = credential.content.get('clear_password')
                logger.debug("Found API Key")

        pool = ThreadPoolExecutor(self.threads)
        def haversine_area(lat1, lon1, lat2, lon2, unit):
            #A = 2*pi*R^2 |sin(lat1)-sin(lat2)| |lon1-lon2|/360
            #A  = (pi/180)R^2 |sin(lat1)-sin(lat2)| |lon1-lon2|
            r = 3959 if unit == "mi" else 6371
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            #logger.debug("haversine_area")
            #logger.debug(lat1)
            #logger.debug(lon1)
            #logger.debug(lat2)
            #logger.debug(lon2)
            return  (((pi/180)*r**2)*abs(math.sin(lat1)-math.sin(lat2))*abs(lon1-lon2))*r
            #return r**2 * abs(math.sin(lat1) - math.sin(lat2)) * abs(lon1 - lon2)
	
        def geocoding_query(record):
            # https://developers.google.com/maps/documentation/geocoding/intro#Types
            output_fields = [
                "json",
                "time_ms",
                "msg",
                "lat",
                "lon",
                "viewport_ne_lat",
                "viewport_ne_lon",
                "viewport_sw_lat",
                "viewport_sw_lon",
                "viewport_area",
                "formatted_address",
                "street_number",
                "route",
                "intersection",
                "country",
                "administrative_area_level_1",
                "administrative_area_level_2",
                "administrative_area_level_3",
                "administrative_area_level_4",
                "administrative_area_level_5",
                "colloquial_area",
                "locality",
                "sub_locality_1",
                "sub_locality_2",
                "sub_locality_3",
                "sub_locality_4",
                "sub_locality_5",
                "ward",
                "sublocality",
                "neighborhood",
                "premise",
                "subpremise",
                "postal_code",
                "postal_code_suffix",
                "natural_feature",
                "airport",
                "park",
                "point_of_interest",
            ]

            for key in self.fieldnames:
                # You have to set all possible output fields to ""
                # otherwise if the first row doesn't set the fields
                # then the rest of the rows can't set it.

                values = record[key]

                for output_field in output_fields:
                    field = key + "_" + output_field

                    if values or field not in record:
                        record[field] = []

                if isinstance(values, str):
                    values = [values]

                for value in values:
                    address = value.strip()

                    if address:
                        for output_field in output_fields:
                            field = key + "_" + output_field
                            record[field].append(self.null_value)

                        URL_PARAMS = {
                            "key": self.APIKey,
                        }
                        params = URL_PARAMS.copy()
                        params.update({
                            "address": address
                        })

                        try:
                            start = time.time()
                            r = requests.get(URL_BASE, params=params)
                            record[key + "_time_ms"][-1] = (time.time() - start) * 1000

                            r.raise_for_status()
                            r_json = r.json()
                            status = r_json["status"]
                            record[key + "_json"][-1] = r.text
                            record[key + "_msg"][-1] = status

                            #logger.info("url: " + r.url)
                            #logger.debug("response: " + r.text)
                            #logger.debug("status: " + status)

                            if status == "OK":
                                #logger.debug("result count: " + str(len(r_json["results"])))
                                result = r_json["results"][0]

                                record[key + "_lat"][-1] = result["geometry"]["location"]["lat"]
                                record[key + "_lon"][-1] = result["geometry"]["location"]["lng"]
                                record[key + "_formatted_address"][-1] = result["formatted_address"]

                                lat1 = result["geometry"]["viewport"]["northeast"]["lat"]
                                lon1 = result["geometry"]["viewport"]["northeast"]["lng"]
                                lat2 = result["geometry"]["viewport"]["southwest"]["lat"]
                                lon2 = result["geometry"]["viewport"]["southwest"]["lng"]

                                record[key + "_viewport_ne_lat"][-1] = lat1
                                record[key + "_viewport_ne_lon"][-1] = lon1
                                record[key + "_viewport_sw_lat"][-1] = lat2
                                record[key + "_viewport_sw_lon"][-1] = lon2
                                record[key + "_viewport_area"][-1] = haversine_area(lat1, lon1, lat2, lon2, self.unit)

                                for component in result["address_components"]:
                                    name = component["types"][0]

                                    if name in output_fields:
                                        record[key + "_" + name][-1] = component["long_name"]

                        except requests.exceptions.HTTPError as err:
                            record[key + "_msg"][-1] = err
                            pass
                        except requests.exceptions.RequestException as e:
                            record[key + "_msg"][-1] = e
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

if __name__ == "__main__":
    dispatch(geocodingCommand, sys.argv, sys.stdin, sys.stdout, __name__)

if __name__ == "__GETINFO__":
    dispatch(geocodingCommand, sys.argv, sys.stdin, sys.stdout, __name__)

