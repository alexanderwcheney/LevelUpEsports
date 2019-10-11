import boto3
import s3fs
import pandas as pd
import sqlalchemy
import keyconfigs as cfg

#Read Contents of S3 Bucket
s3 = boto3.resource('s3')
s3bucket_clean = cfg.s3['clean']
read_bucket = s3.Bucket(s3bucket_clean)

#Create MySQL RDS Connection
database_connection = sqlalchemy.create_engine(
	'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(
		cfg.mysql['database_username'], 
		cfg.mysql['database_password'],
		cfg.mysql['database_ip'], 
		cfg.mysql['database_name']
		)
	)

#Read each file from S3, write to MySQL RDS
for obj in read_bucket.objects.all():
    
    key = obj.key
    body = obj.get()['Body']
    
    df = pd.read_csv(body)
    df.to_sql(
    	con=database_connection,
    	name = obj.key[:-4],
    	if_exists='replace',
    	index=False
    	)
