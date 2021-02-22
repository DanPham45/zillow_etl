import configparser
import psycopg2
from sql_queries import drop_fact_queries, create_fact_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

def drop_fact_tables(cur,conn):
	"""
	- drop existing tables (if any)
	"""
	for query in drop_fact_queries:
		cur.execute(query)
		conn.commit()

def create_fact_tables(cur,conn):
	"""
	- creates dimension tables in Redshift
	"""
	for query in create_fact_queries:
		cur.execute(query)
		conn.commit()


def main():

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_fact_tables(cur, conn)
    create_fact_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()