# Cloud Data Warehouse for Music Streaming Application - Sparkify

## Data Warehouse Design

### Staging Tables
* staging_events
* staging_songs

### Fact Table
* songplays - Event data tracking song plays. Limited to records with event "NextSong" from Staging table.

### Dimension Tables
* users - Contains User Information.
* songs - Contains Songs Information.
* artists - Contains Artists Information.
* time - Contains Time Information.

## Files in Directory
* [config.cfg](./config.cfg): Contains the configuration information for the redshift cluster & related information.
* [sql_queries.py](./sql_queries.py): Contains all the SQL queries needed to create & populate the Sparkify data warehouse.
* [01_create_infrastructure.py](./01_create_infrastructure.py): Contains the code to instantiate the AWS resources for our data processing job.
* [02_create_tables.py](./02_create_tables.py): Contains the functions that run the queries to create and drop the tables.
* [03_etl.py](./03_etl.py): Contains the functions that run the queries to copy data from s3 to staging tables & insert data into the fact and dimension tables.
  

## How to run
0. Input the credentials for the pre-configured IAM user using the command below.

`$ aws configure`
1. Have all the queries that are needed to drop, create, copy & insert the data in relevant tables within the **sql_queries.py**, they can be referenced and run within subsequent python programs.

2. Run the following command to instantiate the AWS resources with necessary configuration for our data processing.

`$ python 01_create_infrastructure.py`

3. Run the following command to set up the schemas for our staging and analytical tables.

`$ python 02_create_tables.py`

4. Run the following command to extract data from the files in S3, stage it in redshift, and finally set it up in the dimensional tables.

`$ python 03_etl.py`

