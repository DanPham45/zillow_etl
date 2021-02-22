#!pip install s3fs
import pandas as pd
import s3fs
import os
import configparser
import psycopg2
from sqlalchemy import create_engine
from sql_queries import drop_dimension_queries, create_dimension_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

def drop_dimension_tables(cur,conn):
	"""
	- drop existing tables (if any)
	"""
	for query in drop_dimension_queries:
		cur.execute(query)
		conn.commit()

def create_dimension_tables(cur,conn):
	"""
	- creates dimension tables in Redshift
	"""
	for query in create_dimension_queries:
		cur.execute(query)
		conn.commit()

def load_dimension_tables():
	"""
	- imports dimension data from S3, writes to Redshift
	"""	

	# read dimension data from S3
	dimension_data = config.get("S3_PATH","dim_input_data")
	xl = pd.ExcelFile(dimension_data)
	d = {}

	# connect to Redshift
	conn_string="postgresql://{}:{}@{}:{}/{}".format(config.get("CLUSTER","DB_USER"),\
			 config.get("CLUSTER","DB_PASSWORD"), config.get("CLUSTER","HOST"),\
			 config.get("CLUSTER","DB_PORT"), config.get("CLUSTER","DB_NAME"))
	conn = create_engine(conn_string)

	# write to Redshift
	tables = ['heating_or_system_type','property_land_use_type','story_type','air_conditioning_type',\
              'architectural_style_type','type_construction_type','building_class_type']
	for i, sheet in enumerate(xl.sheet_names):
	    d[f'{sheet}']= pd.read_excel(xl,sheet_name=sheet)
	    df = d[f'{sheet}']
	    df.to_sql(tables[i], conn, index=False, if_exists='append')


def main():

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_dimension_tables(cur, conn)
    create_dimension_tables(cur, conn)
    load_dimension_tables()

    conn.close()


if __name__ == "__main__":
    main()