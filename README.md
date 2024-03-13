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


