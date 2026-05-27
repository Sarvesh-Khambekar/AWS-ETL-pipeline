import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

#intialize glue job 
args= getResolvedOptions(sys.argv,['JOB_NAME'])
sc= SparkContext()
glueContext= GlueContext(sc)
spark= glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'],args)

# input/output 
input_bucket="aws-etl-pipeline001"
input_prefix= "extracted-data/"
output_bucket="aws-etl-pipeline-cleaned-data"
output_prefix="cleaned_historic-data/"


s3= boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name= "ap-south-1"
)

response= s3.list_objects_v2(
    Bucket=input_bucket,
    Prefix= input_prefix,
    Delimiter='/'    
)

tables = [obj['Prefix'].split('/')[-2] for obj in response.get('CommonPrefixes',[])]
print ("Tables found",tables)



for table in tables:
    print (f"processing table{table}")

    input_path=f"s3://{input_bucket}/{input_prefix}{table}/"
    output_path=f"s3://{output_bucket}/{output_prefix}{table}/"

    df= spark.read.csv(input_path,header= True,inferSchema=True)
# removing duplicate data
    duplicate= df.DropDuplicate()


    #writing back to s3 / cleaned data
    duplicate.write.option("header","True").csv(output_path)

job.commit()

       
    
    







