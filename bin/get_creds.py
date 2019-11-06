import sys, json
import splunk.Intersplunk
import splunklib.client as client
 
results,dummyresults,settings = splunk.Intersplunk.getOrganizedResults()
 
session_key = str(settings.get('sessionKey'))
splunkService = client.connect(token=session_key)
resultsOut = []
 
storage_passwords=splunkService.storage_passwords
 
for credential in storage_passwords:
    if credential.content.get('realm') == "command_geocoding":
        usercreds = {'username':credential.content.get('username'),'password':credential.content.get('clear_password'),'realm':credential.content.get('realm')}
        resultsOut.append(usercreds)
 
splunk.Intersplunk.outputResults(resultsOut)
