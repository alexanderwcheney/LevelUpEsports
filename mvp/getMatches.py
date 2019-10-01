import requests as rq
import sys
import os
import json
import boto3
from datetime import datetime

if __name__ =='__main__':
    esport_name = sys.argv[1]
    api_key = 'api_key'
    req = rq.get("https://api.pandascore.co/"+esport_name+"/matches/past?page[number]=1&token="+api_key)
    body = req.json()
    filename = 'testMVP'+ str(datetime.now(tz=None))+'.json'
    with open(filename, 'w') as outfile:
        json.dump(body, outfile)
    s3 = boto3.client('s3')
    bucket_name = 's3_bucket'

    s3.upload_file(filename, bucket_name, filename)
    os.remove(filename)