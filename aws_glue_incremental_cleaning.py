
import boto3
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")

s

# initialize glue job
try:
      args = getResolvedOptions(sys.argv,['JOB_NAME'])
      sc= SparkContext()
      glueContext = GlueContext(sc)
      spark= glueContext.spark_session
      job = Job(glueContext)
      job.init(args['JOB_NAME'],args)
    
      input_bucket= "aws-etl-pipeline001"
      input_bucket_prefix= f"extracted-data/{today}/"
      output_bucket= "aws-etl-pipeline-cleaned-data"
      output_bucket_prefix= "cleaned_historic-data/"
      
      s3= boto3.client ("s3")
      
      response= s3.list_objects_v2(
      Bucket=input_bucket,
      Prefix= input_bucket_prefix,
      Delimiter='/'    
       )
      tables = [obj['Prefix'].split('/')[-2] for obj in response.get('CommonPrefixes',[])]
      print ("Tables found",tables)

      for table in tables:
        print (f"Processing {table}")
        input_path = f"s3://{input_bucket}/{input_bucket_prefix}{table}/"
        #example = s3://aws-etl-pipeline001/extraced_data/2026-05-10/actor 
        output_path= f"s3://{output_bucket}/{output_bucket_prefix}{table}/"

        if df.rdd.isEmpty():
             print (f"No new data in '{table}' , Skipping upload ")
             continue
        
        df= spark.read.csv (input_path,header= True, inferSchema= True)
        duplicate= df.dropDuplicates()

        duplicate.write\
        .mode("overwrite")\
        .option("header","True")\
        .csv(output_path)
finally:
    job.commit()

