import configparser
import os
from pyspark.sql import SparkSession
from sql_queries import transactions_copy
from pyspark.sql.functions import expr,udf,col
import psycopg2
import sys
import pandas as pd

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

jars = [
    "/RedshiftJDBC42-no-awssdk-1.2.51.1078.jar"
]

def create_spark_session():
	spark = SparkSession \
	    .builder \
	    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
	    .config("spark.driver.extraClassPath", ":".join(jars)) \
	    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
	    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
	    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
	    .config("spark.hadoop.fs.s3a.path.style.access", True)\
	    .config("com.amazonaws.services.s3.enableV4", True)\
	    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")\
	    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")\
	    .getOrCreate()

	return spark

def count_null_value(df):
    """Count number of null value for id column"""
    return df.filter(df.parcel_id.isNull()).count()

def process_transactions_data(spark, input_data, output_data):
	transactions_data = os.path.join(input_data, 'transactions_*.json')
	# read transaction data:
	tdf = spark.read.option("multiline","true").json(transactions_data)
	# clean data
	tdf = tdf.withColumn("transaction_date", expr("TO_DATE(CAST(UNIX_TIMESTAMP(transactiondate, 'yyyy-MM-dd') AS TIMESTAMP))"))
	tdf = tdf.select(col("parcelid").alias("parcel_id"),col("logerror").alias("log_error"), col("transaction_date"))
    
    # data quality check: 
	if count_null_value(tdf) != 0:
		sys.exit("Parcelid contains null value")
	else:
        # save to S3 if pass quality check
		tdf.write.format('parquet').bucketBy(10, "parcel_id")\
		.sortBy("parcel_id").option("path",os.path.join(output_data, 'transactions'))\
		.saveAsTable('zillowtransactions')


def save_transactions_to_redshift(cur, conn):
	cur.execute(transactions_copy)
	conn.commit()


def main():
	# use Spark to clean raw data, save back to S3
    spark = create_spark_session()

    input_data = "s3a://capstone-rawdata/"
    output_data = "s3a://zillowanalytics"
    
    process_transactions_data(spark, input_data, output_data)    
    # copy from S3 to Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    save_transactions_to_redshift(cur, conn)


    conn.close()

if __name__ == "__main__":
    main()