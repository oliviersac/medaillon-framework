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

## How to create a Data Pipeline


## How Medaillon Architecture is managed


