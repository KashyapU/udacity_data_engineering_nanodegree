# Import packages of interest
import json
import boto3
import psycopg2

from configparser import ConfigParser
from sql_queries import copy_table_queries, insert_table_queries

# Get configuration details
config = ConfigParser()
config.read('config.cfg')

# Set connections to AWS Services
redshift = boto3.client('redshift')
cluster_endpoint_props = redshift.describe_clusters(ClusterIdentifier=config['redshift']['IDENTIFIER'])['Clusters'][0]['Endpoint']

def load_staging_tables(db_cursor, db_connection):
    """
    Function to run the queries within the sql_queries.py file to load staged table
    """
    for query in copy_table_queries:
        db_cursor.execute(query)
        db_connection.commit()


def insert_tables(db_cursor, db_connection):
    """
    Function to run the queries within the sql_queries.py file to insert data into 
    the star schema data model
    """
    for query in insert_table_queries:
        db_cursor.execute(query)
        db_connection.commit()


def main():
    db_connection = \
        psycopg2.connect("host={} port={} dbname={} user={} password={}"\
                        .format(cluster_endpoint_props['Address'],
                                config['redshift']['db_port'],
                                config['redshift']['db_name'],
                                config['redshift']['db_user'],
                                config['redshift']['db_password'])
        )

    db_cursor = db_connection.cursor()
    
    load_staging_tables(db_cursor, db_connection)
    insert_tables(db_cursor, db_connection)

    db_connection.close()

if __name__ == "__main__":
    main()