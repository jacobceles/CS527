# pip3.8 install pandas
# pip3.8 install pymysql
# pip3.8 install sqlalchemy
# pop install boto3

import pandas as pd
from sqlalchemy import create_engine
import boto3

# s3 bucket
bucket = "instacart-527-g2"
files = ['departments.csv', 'aisles.csv', 'products.csv', 'orders.csv', 'order_products.csv',
         'order_products_prior.csv']

host = 'instacart.cze09fdga760.us-east-2.rds.amazonaws.com'
port = '3306'
dbname = 'instacart'
user = 'datastars'
password = 'CS527#Datastars'
sqlEngine = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/' + dbname, pool_recycle=3600)
dbConnection = sqlEngine.connect()

# note: must keep csv files in same directory, or change current path in list
for file_name in files:

    s3 = boto3.client('s3', aws_access_key_id="YOUR_AWS_KEY",
                      aws_secret_access_key="YOUR_AWS_SECRET")

    obj = s3.get_object(Bucket=bucket, Key=file_name)
    data = pd.read_csv(obj['Body'])
    table = file_name.replace('.csv', '')
    print('file:', file_name, '; table:', table)
    try:
        frame = data.to_sql(table, dbConnection, if_exists='append', index=False, chunksize=50000)
    except Exception as ex:
        print(ex)
    print('loaded...')
dbConnection.close()
