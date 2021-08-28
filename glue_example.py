# AWS Glue Multiple Table Copier
# (c) Fractal River, 2021

# 
# Standard AWS Glue imports
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Other libraries
import boto3
import pandas as pd
from py4j.java_gateway import java_import

# Constants
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
AWS_REGION='us-east-1'
GLUE_DATABASE='data'

def get_glue_tables(region=None, database=None):
    # This is needed because get_tables only returns a maximum of 100 results - and there may be 100+ tables
    glue = boto3.client('glue' ,region_name=region)
    paginator = glue.get_paginator('get_tables')
    table_iterator = paginator.paginate(DatabaseName=database)
    table_list=[]
    for tables in table_iterator:
        if len(tables):
            table_list.extend(tables['TableList'])
        else:
            break
    return table_list

#########################################################################
##
##                           MAIN SCRIPT
##
## This gives us access to the arguments passed to the script
args = getResolvedOptions(sys.argv, ['JOB_NAME','URL', 'ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

# Initialize AWS Glue job
sc = SparkContext()
sc.setLogLevel("ERROR")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Import Snowflake connector
# print(f"\nSetting up Snowflake connector...")
# java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
## uj = sc._jvm.net.snowflake.spark.snowflake
# spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
# sfOptions = {
#     "sfURL" : args['URL'],
#     "sfAccount" : args['ACCOUNT'],
#     "sfUser" : args['USERNAME'],
#     "sfPassword" : args['PASSWORD'],
#     "sfDatabase" : args['DB'],
#     "sfSchema" : args['SCHEMA'],
#     "sfWarehouse" : args['WAREHOUSE'],
# }

# Get names of all the tables from the Glue Data Catalog
print("\nReading tables from Glue catalog...")
table_list = get_glue_tables(region=AWS_REGION, database=GLUE_DATABASE)

print(f"\nProcessing {len(table_list)} tables:")
for table in table_list:
    tableName = table['Name']
    print('\nProcessing: '+tableName)
    # Read table from Postgres RDS
    print('\n-- Reading from Posgres: '+tableName)
    dyf0 = glueContext.create_dynamic_frame.from_catalog(database = GLUE_DATABASE, table_name = tableName, transformation_ctx = "dyf0")
    # Convert to DataFrame
    df = dyf0.toDF()
    # Work with the DataFrame
    df.printSchema()
    pdf = df.toPandas()
    print("\n-- Now this is a Pandas dataframe!")
    print(pdf)
    # After working with it, convert back to DataFrame
    df2 = spark.createDataFrame(pdf)
    # DynamicFrame
    # dyf2 = DynamicFrame.fromDF(df2, glueContext, "dyf2")

    # Write table to Snowflake
    print('\n-- Writing to Snowflake: '+tableName)
    print(f"\nOptions: {sfOptions}")
    ## Write the Data Frame contents back to Snowflake in a new table df1.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "[new_table_name]").mode("overwrite").save() job.commit()
    # df3 = df2.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", tableName).mode("overwrite")
    # df3.save()

print(f"\nJob completed.\n")

job.commit()
