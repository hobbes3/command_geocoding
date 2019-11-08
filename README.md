# Splunk Geocoding Command
Authors: Satoshi Kawasaki, Ryan O'Connor

### Features
* Lookup any address, lat/lon pair, or other geographical location
* Supports Splunk multivalue fields 
* Supports multithreading
* Secure storage of API Key in Splunk Password Store

### Usage
`| makeresults | eval s="270 brannan st SF" | geocoding s`

### Options
* `... | geocoding threads=16 s`. Values allowed: positive integers. Defaults to `threads=4`.
* `... | geocoding null_value="N/A" s`. Values allowed: any string. Used when a field has no value. Especially useful to align all multivalue inputs and outputs neatly. Defaults to `null_value=""`. 
* `... | geocoding unit=km s`. Values allowed: `mi` or `km`. Used only for the `_viewport_area` value. Defaults to `unit=mi`.
