# Import packages of interest
import json
import boto3
import psycopg2

from configparser import ConfigParser
from sql_queries import create_table_queries, drop_table_queries

# Get configuration details
config = ConfigParser()
config.read('config.cfg')

# Set connections to AWS Services
redshift = boto3.client('redshift')
cluster_endpoint_props = redshift.describe_clusters(ClusterIdentifier=config['redshift']['IDENTIFIER'])['Clusters'][0]['Endpoint']

def drop_tables(cur, conn):
    """
    Function to run the queries within the sql_queries.py file to drop existing tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Function to run the queries within the sql_queries.py file to create table based on schema provided
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    db_connection = \
        psycopg2.connect("host={} port={} dbname={} user={} password={}"\
                        .format(cluster_endpoint_props['Address'],
                                config['redshift']['db_port'],
                                config['redshift']['db_name'],
                                config['redshift']['db_user'],
                                config['redshift']['db_password']))
    db_cursor = db_connection.cursor()

    drop_tables(db_cursor, db_connection)
    create_tables(db_cursor, db_connection)

    db_connection.close()

if __name__ == "__main__":
    main()