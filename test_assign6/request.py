


#from json_with_dates import loads
#import json_with_dates
import json
import logging

def request_to_measurements(path=""):
    from http.client import HTTPConnection, HTTPResponse
    logging.debug('path'+path)
    hconn = HTTPConnection('localhost', 12345)
    hconn.request('GET',  path)
    resp = hconn.getresponse()
    logging.debug('status'+str(resp.status))
    bodyJ = resp.read()
    logging.debug('body'+str(bodyJ))
    bodystr = bodyJ.decode()
    if resp.status == 200:
        body = json.loads(bodystr)
        return ('K',body)
    else:
        return ('E', resp.status, bodystr)


def request_or_fail(path=""):
    resp = request_to_measurements(path)
    if resp[0] == 'K':
        return resp[1]
    else:
        raise Exception(str(resp[1]) + " " + str(resp[2]))

