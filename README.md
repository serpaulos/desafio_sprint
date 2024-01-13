# Spring Data Engineer Challenge
### ** ONGOING project **

> Project sugested by https://datasprints.com/ as a challenge for aspiring Data Engineer. <br>
> 

## Contents
- [Objective](#Objectives)
- [General Information](#General-information)
- [Technologies](#Technologies)
- [Project Status](#Project-Status)
- [Project Details](#project-details)
- [Improvements](#improvements)
- [Contact](#Contact)
<!-- * [License](#license) -->

## Objectives
- The purpose of this test is to assess your proficiency in the basic requirements, such as: <br>
- Basic programming with SQL
- Basic programming with Python
- Experience with Cloud Computing
- Experience with Linux
- Experience with Data Science/Engineering

## General-information
- Data from NYC Taxi Trips was used in order to perform this project.
- 3 different sets of data was used like:

| Dataset                                 | Description | 
|-----------------------------------------|:-----------:|
| [2009](#) [2010](#) [2011](#) [2012](#) | Data on taxi rides in ​New York |
| [Vendor Lookup](#)                      | Data on taxi service companies      |
| [Payment Lookup](#)                     | are Map between prefixes and actual payment types      |

## Deliverables
# Minimun 
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

## Libraries Used (present in the requirements.txt)
- pandas==2.1.4
- prefect==2.14.13
- prefect_gcp==0.5.5
- prefect_sqlalchemy==0.3.2
- PyMySQL==1.1.0
- Requests==2.31.0
- SQLAlchemy==1.4.51


## Project-Status
_ongoing_

# Project-Details

* Containers created to implement the project using Docker-compose yaml files
    * Mysql

* Stand alone features (servers)
    * Prefect
    * Jupyter Notebook

* Project folder structure
    * Prefect
      * flows (folder were files to be used in the flow/ETL are stored
           * data (store data when downloaded on the ETL process)
           * Prefect Blocks were used to configure the credentials to access the GCP
           * GCP Credentials | GCS Bucket | SQLAlchemy Connector
           * .env files were used to manage some credentials as well
      * Data_infra (data docker use to store Mysql data 
      * Exploration (Store notebooks used to prototype the ETL)

- jupyter notebookS (Explanation)
  * answer_questions.ipynb - used to answer  the proposed questions
  * exploration_data_vendor.ipynb - used to prototype the ingestion of data

- Files in Flow folder
    * ingest_data - First version or prototype to ingest data to database
    * ingest_data_prefect - ETL file with just one variable, not configured to accept more than one variable
    * ingest_data_prefect_payment - ingest data to the database for payment table
    * ingest_data_prefect_vendor - ingest data to the database for vendor table
    * parameterized_flow_data_sample - Final file to process data with more than one variable

- Prefect ETL sequence
  * download_data -> extract_data -> transform_data -> write_local -> write_gcs -> ingest_data
  * Run was configure to accept parameters, you can put more then one year as a parm to the funcion and it will process it as list

## Improvements
* Project still ongoing, since I am studying more about the Data Engineer roles.
* Need to study more about stream ETL for example.
* Trere is an Airflow version at the repository, when its done I will complement the Radme file


## Contact
made by [@paulosilvajr](https://www.linkedin.com/in/paulosilvajr/) - feel free to contact me if you need!