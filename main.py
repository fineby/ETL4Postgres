# import Python libraries and connection
import boto3
import pandas as pd
import numpy as np
import json
import cryptocode
import db

# masked column
def crypt(column): 
    return df[column].apply(lambda x: cryptocode.encrypt(x,"password")) 
    # Decode
    #decoded = cryptocode.decrypt(encoded, "password")
 
# load and delete from local sqs queue with save to Pandas dataframe
sqs = boto3.client("sqs", region_name="", aws_access_key_id="", aws_secret_access_key="",endpoint_url='http://localhost:4566')
queue_url = "http://localhost:4566/000000000000/login-queue"

df = pd.DataFrame() 

while True:
    messages = sqs.receive_message( # bath of messages from the queue
        QueueUrl=queue_url,
        MaxNumberOfMessages=10)

    if 'Messages' in messages: # looping in list of messages
        for message in messages['Messages']:  
            jdata = json.loads(message['Body']) # creating JSON object with data
            df_temp = pd.DataFrame.from_dict([jdata], orient='columns') # generating one row DF
            df=pd.concat([df,df_temp], ignore_index=True) # concat rows in cycle to build final DF
            sqs.delete_message( # deleting message on server if it already loaded to DF
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle'])
    else:
        break # break cycle when all messages loaded to DF (no messages in bath)

# dropping extra columns
df.drop(df.columns[[6,7]], axis=1, inplace=True)

# changing to 'no value'(None) for DF, drop rows without data and reset index for DF
df.replace({np.nan: None}, inplace=True)
df.dropna(axis=0, how='all', inplace=True)
df.reset_index(drop=True, inplace=True)

# masked columns
df.insert(loc = 6, column = 'masked_device_id', value = crypt('device_id'))
df.insert(loc = 3, column = 'masked_ip', value = crypt('ip'))

# return None for masked columns
for i in range(df.shape[0]):
    if df['device_id'][i] is None: df['masked_device_id'][i] = None
    if df['ip'][i] is None: df['masked_ip'][i] = None

# drop not masked colums from DF
df.drop(columns={'ip','device_id'} ,axis=1, inplace=True)

# delete '.' for the column's data for type Int in DB schema
df.loc[:,'app_version']=df.loc[:,'app_version'].apply(lambda x: str(x).replace('.', '')) #replace({'.': ','}, inplace=True)

# save DF to DB
db.save(df)