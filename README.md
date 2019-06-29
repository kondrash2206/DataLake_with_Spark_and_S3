# DataLake_with_Spark_and_S3
Querying S3 Datalake filled with json files to create a relational database

# Description
In this project I have created an ETL Pipeline for an imaginary music streaming app Sparkify.
First, I have extracted data from a DataLake which is located in Udacity public AWS S3 bucket "udacity-dend" containing multiple .json files. Json files were obtained from ["Million Song Dataset"](http://millionsongdataset.com/) and ["Music streaming app event simulator"](https://github.com/Interana/eventsim). 

Then I used this data to create a star chema tables. Fact Table: **songplays**; Dimension Tables: **users**,**songs**, **artists**, **time**. These tables were then saved in form of structured parquet files making a Data Warehouse. This is of huge benefit for an imaginary music streaming app "Sparkify" due to following reasons:
* Provides information about user activities over times of day and user location which makes possible to correctly arrange resources
* Provides information about user song and band preferences which allow to develop a recommendation system (rank based recomendations, user-user collaborative filtering e.t.c)
* Provides information about user subscriptions. The behaviour of users that cancelled the subscription can be analysed and this knowldege can be used to identify users that might canceln their membership in the future. Such users could recieve some benefits (discounts, gifts etc) which prevents the cancelation of their membership. 

# Database Schema 
The star schema below allows to answer the above stated questions:
![](https://github.com/kondrash2206/DataLake_with_Spark_and_S3/blob/master/schema.png)

# ETL Pipeline
ETL Pipeline gets the data from 2 datasources (Staging Tables) and fills 4 Dimension Tables (**Songs**, **Artists**, **Time**, **User**). The Fact Table **Songplays** is filled also from **Songs** and **Artists** Tables.
![](https://github.com/kondrash2206/Data_Modeling_with_Postgres/blob/master/ETL.png)

# Files
* **etl.py** - ETL (Extract Transfer Load) Pipeline that reads all files from datalake, transform them into star schema tables and loads them into S3 Bucket.
* **dl.cfg** - configuration file, containing the information needed to work with S3. 

# Installations
In order to run this project following python libraries are needed: pyspark, os, datetime, configparser. To run it first start add all necessary information into "dl.cfg" file and then run "etl.py" that fills the tables with data. 

### Acknowledgements
This project is a part of Udacity "Data Engineering" Nanodegree
