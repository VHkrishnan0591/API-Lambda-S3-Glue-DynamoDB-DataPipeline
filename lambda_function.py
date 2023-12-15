#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
import json
import requests
import json
import pymongo
from pymongo import MongoClient
import datetime
from datetime import date

#Creating a Database:
def create_database(name):
    return client[name]

#Creating a Collection:
def create_collection(Database,name):
    return Database[name]

#API Invoking Function
def api_call_function(from_date1,to_date1):
    DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"                   f"?date_received_max=<todate>&date_received_min=<fromdate>"                   f"&field=all&format=json"
    fromdate1 = from_date1
    todate1 = to_date1
    url = DATA_SOURCE_URL.replace("<todate>", todate1.split(" ")[0]).replace("<fromdate>",fromdate1.split(" ")[0])
    data = requests.get(url)
    finance_complaint_data = []
    if data.status_code == 200:
        finance_complaint_data = list(map(lambda x: x["_source"],filter(lambda x: "_source" in x.keys(),json.loads(data.content))))
        print("API Response is successful")
    else:
        print(f"Error: {response.status_code}")
    return finance_complaint_data

#Writing in the Amazon S3

def writing_On_Amazon_S3(data,fromdate,todate):
    #Creating Session With Boto3.Create a new user with IAM policy as S3FullAccess
    session = boto3.Session(
    aws_access_key_id='AKIAWLK7SBUFXCBUVMFB',
    aws_secret_access_key='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'
    )

    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    from_date1 = fromdate
    to_date1 = todate
    Key=str(from_date1)+" "+"to"+" "+to_date1+"finance_complaint_data.json"
    object = s3.Object('practiseons3', Key)
    finance_complaint_data = data
    result = object.put(Body=(bytes(json.dumps(finance_complaint_data).encode('UTF-8'))))
    res = result.get('ResponseMetadata')

    if res.get('HTTPStatusCode') == 200:
        print('File Uploaded Successfully')
    else:
        print('File Not Uploaded')


def lambda_handler(event, context):
    # TODO implement
    db_name = 'Finance'
    collection_name = 'Complaints'

    # Connect to MongoDB Atlas cluster
    client = pymongo.MongoClient(
        "mongodb+srv://Leo:Welcome-123@cluster0.aa7wqoc.mongodb.net/?retryWrites=true&w=majority"
    )
    #checking whether the database exists
    db_names = client.list_database_names()
    if db_name in db_names:
        db = client[db_name]
        print("Already Database exsists")
    else:
        db = create_database(db_name)
        print("Database created")

    #checking whether collection exists
    if collection_name in db.list_collection_names():
        collection = db[collection_name]
        print("Collection already exists")
    else:
        collection = create_collection(db,collection_name)
        print("collection created")
    
    
    if (collection.distinct("from_date") == []) & (collection.distinct("to_date") == []):
        from_date = "2023-11-15"
        from_date = str(datetime.datetime.strptime(from_date, "%Y-%m-%d"))
#         to_date = "2023-12-03"
        to_date = date.today()
        to_date = str(datetime.datetime.strptime(str(to_date), "%Y-%m-%d"))
        data = api_call_function(from_date,to_date)
        data1 = {"from_date":from_date, "to_date":to_date}
        collection.insert_one(data1)
        writing_On_Amazon_S3(data,from_date,to_date)
    
    else:
        to_date1 = date.today()
        to_date1 = str(datetime.datetime.strptime(str(to_date1), "%Y-%m-%d"))
#         from_dates = list(collection.find().sort({"to_date":-1}))
        from_dates = list(collection.find().sort([("to_date", -1)]))
        api_dates = from_dates[0]
        from_date1 = str(api_dates['to_date'])
        if from_date1 == to_date1:
            print("Data Already Uploaded into Amazon S3")
        else:
            print(from_date1.split(" ")[0])
            data = api_call_function(from_date1,to_date1)
            data1 = {"from_date":from_date1, "to_date":to_date1}
            collection.insert_one(data1)
            writing_On_Amazon_S3(data,from_date1.split(" ")[0],to_date1.split(" ")[0])
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


# In[ ]:




