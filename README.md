# datalake-test

## Description

This service handles data ingestion and data transformations based on the medaillon architecture for Data-Lakes. 

## Local Setup
Local setup is not defined yet


## Main Components

### Handlers
Handlers are managing data transformations on dataframes and other types of conversions. In most pipelines, the handlers will filter data or dedupe data. Basically the responsibility of a handler is to modify data.

### Pipelines
The pipelines are basically just defined by the transformation that needs to be done. For instance, between bronze and silver, there is a need for data filtering and deduplication. The filtering rules and de deduplication rules are defined in a pipeline component

### Readers

### Writers

### Jobs

### Transfer Log table
The transfer log table is a key component in any pipeline mechanisms. It logs all operations that occurs between the source and the final layer destination.


## ITS INCOMPLETE
| Column  | Description |
| ------------- | ------------- |
| origin_type  | The type of the origin (S3, delta_table, source)  |
| origin_name  | The name of the origin (dev_bronze, dev_silver, dev-landing)  |
| origin_table  | The table of the origin (dev.dev_bronze.stocks)  |
| destination_type  | The type of the destination (dev_bronze, dev_silver, dev-landing)  |
| destination_name  | The name of the destination (S3, delta_table, source)  |
| destination_table  | The table of the destination (stocks)  |
| schema_used  | The schema that was used to store the data  |
| rows_received  | The number of rows that were received for processing  |
| rows_processed  | The number of rows that were processed  |
| processing_time  | The date that the processing was done  |
| transfer_status  | SUCCESS or FAIL after processing  |
| failed_reason  | The reason why the transfer failed  |




## How to create a Data Pipeline from an S3 Event Bucket

### 1. Define the Schema with the stakeholders
This is done by bla

### 2. Identify the source for pulling the data
This is done by bla

### 3. Define the transformations needed between bronze and silver
This is done by bla

### 4. Define the transformations needed between silver and gold
This is done by bla

### 5. Create a job for the full workflow
This is done by bla



## How Medaillon Architecture is managed


