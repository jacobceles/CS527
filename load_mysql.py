import pandas as pd
from sqlalchemy import create_engine

host = 'instacart.cze09fdga760.us-east-2.rds.amazonaws.com'
port = '3306'
dbname = 'instacart'
user = 'datastars'
password = 'CS527#Datastars'
sqlEngine = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/' + dbname, pool_recycle=3600)
dbConnection = sqlEngine.connect()

files = ['departments.csv', 'aisles.csv', 'products.csv', 'orders.csv', 'order_products.csv',
         'order_products_prior.csv']

# note: must keep csv files in same directory, or change current path in list
for file in files:
    table = file.replace('.csv', '')
    print('file:', file, '; table:', table)
    data = pd.read_csv(file)
    try:
        frame = data.to_sql(table, dbConnection, if_exists='append', index=False, chunksize=50000)
    except Exception as ex:
        print(ex)
    print('loaded...')
dbConnection.close()
