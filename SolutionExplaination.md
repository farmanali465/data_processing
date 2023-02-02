# AirDNA Data Engineering Project

I have got a task to create a data pipeline with following requirements:

1) Ingest of properties and amenities into a parquet format (locally)
2) Creation of two data outputs leveraging the parquet data (also locally)

I have created generic utilities by thinking future scenarios to read and write.There is generic Spark
Session `SparkSessionCreator`
is created so there will no need to create in every job.
I have created  `DataExtrcator` and `DataLoader`,`DataTransfomrer` with following details:

`DataExtrcator`  : Contains different utility functions to load data from local (e.g CSV,JSON,Parquet).We can extend
this by creating new functions to different formats if we want.

`DataTransfomrer` : Contains on transformation whatever we are performing against business logic and data cleaning.

`DataLoader` : Contains different utility functions to write data in local (e.g CSV,JSON,Parquet).We can extend
this by creating new functions to different formats if we want.

`PipelineJob` : Used to execute our pipeline.

`TestSpecs` : Used to execute tests cases.

We have created all of our data outputs in `output` folder.

## Improvements

We can perform following improves in current solution:

- We need to add partition key on date while writing amenities.So its easy and fast to get data for specific date
- We need to write logging into some database for tracking propose
- We need to use orchestrate tool to run this job
- We need to add some email solution so whenever our job fails we can receive notification
- while joining we need to broadcast smaller dataset for better performance

There are more improvement we can perform 

