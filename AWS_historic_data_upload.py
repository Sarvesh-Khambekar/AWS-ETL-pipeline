import mysql.connector 
import pandas as pd
import boto3
from io import StringIO

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root7",
    "database": "sakila"
}

conn = mysql.connector.connect(**db_config)

s3_bucket="aws-etl-pipeline001" #bucket name
s3_prefix="extracted-data/" #folder name 
i=0

s3 = boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="ap-south-1"
)
# fetching only base tables 
try:  
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'sakila'
    AND table_type = 'BASE TABLE';
    """
    tables_df = pd.read_sql(query, conn)
    tables=tables_df['table_name'].tolist()
    print(f"Found {len(tables)} tables")

    # proccessing tables
    for table in tables:
       print(f"Processing table: {table}")

    # load table 
       df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # converting to csv 

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    

#upload to s3
    s3.put_object(
         Bucket=s3_bucket,
         Key=f"{s3_prefix}{table}{table}.csv",
         Body=csv_buffer.getvalue()
         )
    print(f"Uploaded {table}.csv to S3")
except:
    print("Error")
finally:
    conn.close()







