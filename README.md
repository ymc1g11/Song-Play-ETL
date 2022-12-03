# Summary

This project aims to help the Sparkify analytical team to gain some insights into the data that the team has been collecting. The purpose of the Database is to help the team to understand what songs users are currently listening to by using query structure as simple as possible, while keeping the Database organised. Here, we are using the a Star-Schema to design our database as it is a schema suitable for designing a OLAP database.

# Files

## Data Files

Two types of files in JSON format are provided - a file containing songs data and a file containing event logs. We extract our table data from these 2 type of files.

## Python Scripts

To setup and populate the database, run the create_tables.py script, followed by etl.py.

### sql_queries.py

This file defines all the query templates, which includes CREATE, INSERT and SELECT queries. The INSERT and SELECT queries are query templates. This script should not be run specifically, instead, serves as a library of sql queries and templates that is referenced by other python scripts in this project.

### create_tables.py

This is the starting point of the ETL pipeline development. This script is to create and/or reset the database.

### etl.py

This is the main extract, transform and load step. This script contains function to process the data files separately. For each file processing function, data is loaded into DataFrame. Data is processed within the DataFrame and loaded into the individual tables. This script must only be run after create_tables.py has been run.

