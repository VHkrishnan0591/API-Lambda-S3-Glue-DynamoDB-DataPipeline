API-Lambda-S3-Glue-DynamoDB 
This repo presents an enhanced version of the Lambda-S3-Glue-DynamoDB project. The primary objective of this project is to retrieve data from an API source, specifically the Consumer Finance Data API and use some AWS Services to ingest the Data to an AWS DynamoDB.
Project Architecture:
![image](https://github.com/VHkrishnan0591/API-Lambda-S3-Glue-DynamoDB-DataPipeline/assets/153942112/57ea97cc-66ff-46ed-beb1-b29566f91c58)

The API's URL includes parameters to define the desired date range for data extraction. Here is an example format for the used API request:

https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2023-01-01&date_received_min=2023-02-01&field=all&format=json
Tech Stack
MongoDB
Python
Spark
AWS S3
AWS Lambda
AWS Event Bridge
AWS Cloud Watch
AWS IAM
AWS Glue
AWS DynamoDB

Workflow

The project workflow can be divided into two distinct parts. Firstly, an AWS EventBridge is configured to execute on a predefined schedule, triggering an AWS Lambda function. This Lambda function is responsible for retrieving data from the API and storing it in AWS S3. Additionally, metadata regarding the extracted data, such as the start and end dates, is stored in a MongoDB database. This prevents the duplication of data during subsequent runs of the Lambda function.

To work with the project's code, import the lambda_function_code.zip file into the Lambda Function. It contains necessary library dependencies to enable the aforementioned functionality.

Secondly, the data stored in S3 is transferred to DynamoDB through an AWS Glue Job. Prior to initiating the Glue Service, it is essential to create a target table in DynamoDB where the data will be loaded. The Glue service involves the creation of a Crawler, which scans the specified S3 data location and extracts metadata information, such as the data schema, storing it in AWS Athena DB.

The Glue Job, which encompasses the Crawler, can be triggered either on demand or based on a predefined schedule. To facilitate this, the Glue Job is integrated with the Workflows functionality provided by AWS Glue.

The Glue Job itself is a Python script that retrieves data from S3 and loads the incremental data into DynamoDB. To enable incremental loading into the next service, it is crucial to enable job bookmarking in the Glue Service.

Refer the Link for collection of screenshots captured during the project execution
