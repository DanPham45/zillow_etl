import configparser
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from sql_queries import details_copy, drop_detail_columns
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


def count_column_types(spark_df):
    """Count number of columns per type"""
    return pd.DataFrame(spark_df.dtypes).groupby(1, as_index=False)[0].agg({'count':'count'}).rename(columns={1:"type"})

def count_null_value(df):
    """Count number of null value for id column"""
    return df.filter(df.parcelid.isNull()).count()

def process_details_data(spark, input_data, output_data):
	properties_data = os.path.join(input_data, "properties_*.csv")
	# read property data
	pdf = spark.read.option("header","true").csv(properties_data)
	# drop unimportant columns
	pdf = reduce(DataFrame.drop, drop_detail_columns, pdf)
	# convert columns to int
	pdf = pdf.select(*(col(c).cast("int").alias(c) for c in pdf.columns))
	#drop columns with null parcelid
	pdf = pdf.na.drop(subset=["parcelid"])
	
    
    # data quality check: 
	if count_column_types(pdf)['count'][0] != 17:
		sys.exit("Did not have 17 columns as defined")
	elif count_column_types(pdf)['type'][0] != 'int':
		sys.exit("Did not have int type")
	elif count_null_value(pdf) != 0:
		sys.exit("Parcelid contains null value")
	else:
        # save clean result back to S3 if pass all checks
		pdf.write.format('parquet').bucketBy(10, "parcelid")\
		.sortBy("parcelid").option("path",os.path\
		.join(output_data, 'propdetails')).saveAsTable('properties')

def save_details_to_redshift(cur, conn):
	cur.execute(details_copy)
	conn.commit()


def main():
	# use Spark to clean raw data, save back to S3
    spark = create_spark_session()

    input_data = "s3a://capstone-rawdata/"
    output_data = "s3a://zillowanalytics"
    
    process_details_data(spark, input_data, output_data)    
    # copy from S3 to Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    save_details_to_redshift(cur, conn)


    conn.close()

if __name__ == "__main__":
    main()