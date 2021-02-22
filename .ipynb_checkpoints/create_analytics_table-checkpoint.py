import configparser
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from sql_queries import analytics_copy, transactions_by_month
from pyspark.sql.functions import expr,udf,col
import psycopg2


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


def process_analytics_data(spark, input_data, output_data):
	trans = spark.read.format("jdbc")\
	.option("url","jdbc:redshift://{}:{}/{}"\
	.format(config.get("CLUSTER","HOST"),config.get("CLUSTER","DB_PORT"),config.get("CLUSTER","DB_NAME")))\
	.option("driver","com.amazon.redshift.jdbc42.Driver")\
	.option("dbtable","property_transactions")\
	.option("user", config.get("CLUSTER","DB_USER"))\
	.option("password", config.get("CLUSTER","DB_PASSWORD"))\
	.load()

	details = spark.read.format("jdbc")\
	.option("url","jdbc:redshift://{}:{}/{}"\
	.format(config.get("CLUSTER","HOST"),config.get("CLUSTER","DB_PORT"),config.get("CLUSTER","DB_NAME")))\
	.option("driver","com.amazon.redshift.jdbc42.Driver")\
	.option("dbtable","property_details")\
	.option("user", config.get("CLUSTER","DB_USER"))\
	.option("password", config.get("CLUSTER","DB_PASSWORD"))\
	.load()
	
	# create temp tables
	trans.createOrReplaceTempView("transactions")
	details.createOrReplaceTempView("details")
	
	# find transaction count and transaction value by month by region
	analytics_table = spark.sql(transactions_by_month)
	analytics_table.write.mode('overwrite')\
	.parquet(os.path.join(output_data, 'transactions_by_month'))

def save_results_to_redshift(cur, conn):
	cur.execute(analytics_copy)
	conn.commit()


def main():
	# use Spark to clean raw data, save back to S3
    spark = create_spark_session()

    input_data = "s3a://capstone-rawdata/"
    output_data = "s3a://zillowanalytics"
    
    process_analytics_data(spark, input_data, output_data)    
    # copy from S3 to Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    save_results_to_redshift(cur, conn)


    conn.close()

if __name__ == "__main__":
    main()