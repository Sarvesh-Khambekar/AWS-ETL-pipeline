import pymysql
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# creating empty list to store table with last update,last update is the only column which is used indentify which data is new
tables_with_last_update= []
tables_without_last_update=[]

# establishing connection to mysql via python
connection = pymysql.connect(
    host="localhost",        # e.g. localhost or RDS endpoint
    user="root",
    password="root7",
    database="sakila",
    cursorclass=pymysql.cursors.Cursor
)

# accessing aws s3 via boto3 lib
# s3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id= "",
    aws_secret_access_key="",
    region_name= "ap-south-1"
)
database= "sakila"
bucket_name= "aws-etl-pipeline001"
prefix_name= "extracted-data/"
today = datetime.now().strftime("%Y-%m-%d")

try:
    # sql query which will only return base table 
    with connection.cursor() as cursor:
       cursor.execute("""         
           SELECT table_name
           FROM information_schema.tables
           WHERE table_schema= 'sakila'
           AND table_type = 'BASE TABLE'     
        """)
       tables = [row[0] for row in cursor.fetchall()]
# loop which will check which tables have last update column, and it will append in list
       for table in tables:
          cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'sakila'
            AND table_name = '{table}'
            AND column_name = 'last_update'
          """) 
          count = cursor.fetchone()[0] # list logic if last update column prsent in table , table with last updte col 
          if  count >0:
             tables_with_last_update.append(table)
          else:
             tables_without_last_update.append(table)  

# loop to process the data , can identify in which table data is newly inserted
       for table in tables_with_last_update:
          print (f"processing '{table}'") 

          # read table into Dataframe 
          query = (f"""
            SELECT * FROM {table}
            WHERE last_update>= '{today} 00:00:00' 
            AND last_update< DATE_ADD('{today}',INTERVAL 1 DAY)
            """)
          df= pd.read_sql(query,connection)
# most imp, if the data isn't inserted in tables it won't create folder for those tables which don't have any newly inserted data
          if df.empty:
             print (f"No new data in '{table}' , Skipping upload ")
             continue

          # convert Dataframe into csv in memory
          csv_buffer= StringIO()
          df.to_csv(csv_buffer,index=False)

            # upload to s3
          s3.put_object(
          Bucket= bucket_name,
          Key= f"{prefix_name}{today}/{table}/{table}.csv",
          Body=  csv_buffer.getvalue()
          )             
finally:
   connection.close()
 



