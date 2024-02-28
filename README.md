# Spring Data Engineer Challenge

> Project sugested by https://datasprints.com/ as a challenge for aspiring Data Engineer. <br>
> 

## Objectives
- This test was provided by Datasprint to assess your proficiency in the basic requirements, such as: <br>
- Basic programming with SQL
- Basic programming with Python
- Experience with Cloud Computing
- Experience with Linux
- Experience with Data Science/Engineering

# Project-Details
- This ETL test involve extracting data from provided datasets listed in table below.
- Data has some inconsistencies related to datetime columns and some "dirty" data.

## General-information
- Data from NYC Taxi Trips was used in order to perform this project.
- 3 different sets of data was used like:

| Dataset                                 | Description | 
|-----------------------------------------|:-----------:|
| [2009](#) [2010](#) [2011](#) [2012](#) | Data on taxi rides in ​New York |
| [Vendor Lookup](#)                      | Data on taxi service companies      |
| [Payment Lookup](#)                     | are Map between prefixes and actual payment types      |

## Deliverables
- This is the basic/expected deliverables to be sent by the assigned professional being tested.
### Questions: 

- What is the average distance covered by trips with a maximum of 2 passengers;
- What are the 3 biggest sellers in terms of total amount of money raised;
- Make a histogram of the monthly distribution, over the 4 years, of races paid in cash;
- Make a time series graph counting the amount of tips each day, in
last 3 months of 2012.

## Technologies
- Python
- Pandas
- Prefect
- Docker (Docker compose)
- GCP Bucket
- Mysql

# Usage
- To reproduce this test you can clone the project using this link https://github.com/serpaulos/sprint_challenge
- Docker compose needed to implement this project. Inside the folder prefect you can find the .env_model and dockerdocker-compose.yml.
- Rename the .env_model to .env and change the variables accordingly.
- Review and change the dockerdocker-compose volumes to your structure.

## Files in prefect/flows folder
    * ingest_data - First version or prototype to ingest data to database
    * ingest_data_prefect - ETL file with just one variable, not configured to accept more than one variable
    * ingest_data_prefect_payment - ingest data to the database for payment table
    * ingest_data_prefect_vendor - ingest data to the database for vendor table
    * parameterized_flow_data_sample - Final file to process data with more than one variable

- Prefect ETL sequence
  * download_data -> extract_data -> transform_data -> write_local -> write_gcs -> ingest_data
  * Run was configure to accept parameters, you can put more then one year as a parm to the funcion and it will process it as list


## Contact
made by [@paulosilvajr](https://www.linkedin.com/in/paulosilvajr/) - feel free to contact me if you need!
