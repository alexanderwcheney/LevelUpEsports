import os
import json
import boto3
import requests as rq
from datetime import datetime
import keyconfigs as cfg

if __name__ =='__main__':
    
    #Load configs
    esports_titles = cfg.esports_titles
    api_key = cfg.pandascore_API['api_key']
    s3bucket_raw = cfg.s3['raw']
    
    total_calls = 0
    #Ensure API Call limit is not exceeded
    while total_calls < 1000:
        
        #For every esport
        for esport in esports_titles:
            esport_name = esport
            page = 1
            
            #Request all pages
            while page:
                req = rq.get(
                    ("https://api.pandascore.co/"
                        +esport_name
                        +"/matches/past?page[number]="
                        +str(page)
                        +"&token="
                        +api_key
                        )
                    )

                page+=1
                total_calls+=1                
                
                if req.json():
                    body = req.json()
                    filename = (esport_name
                        +str(datetime.now(tz=None))
                        +'page'
                        +str(page)
                        +'.json')
                    
                    #Write to local file
                    with open(filename, 'w') as outfile:
                        json.dump(body, outfile)
                    #Upload to S3
                    s3 = boto3.client('s3')
                    s3.upload_file(filename, s3bucket_raw, filename)
                    #Remove local file
                    os.remove(filename)
                
                else:
                    break
                    print('request failed')