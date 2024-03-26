# Project Name: Medallion Data Lake Framework

The Medallion Data Lake Framework is a Python-based framework designed to simplify the process of building data lakes using PySpark and Amazon S3. Leveraging the principles of the Medallion architecture, this framework streamlines data ingestion, storage, transformation, and transfer, enabling developers to effortlessly create robust data pipelines.

## Key Features:
- Seamless data ingestion from bucket storage.
- Integration with meta stores for efficient data management.
- Implementation of the bronze, silver, and gold layers for comprehensive data - organization.
- Flexible data transformation and transfer between layers using defined rules.
- Orchestration of pipeline steps through customizable workflows.
- Support for various orchestration tools, such as Databricks workflows.
- Task-based approach for defining pipeline steps, including ingest, transfer, and replay.
- Reusability of tasks to construct comprehensive data pipelines with varying - parameters and transformation rules.
- High reusability, enabling the creation of multiple data lake projects with ease.

## How it Works:
The Medallion Data Lake Framework follows a modular approach, where developers define transformation rules and orchestrate pipeline steps to move data between layers. By encapsulating common tasks into reusable components, developers can rapidly construct and customize data pipelines for specific project requirements.

For more details about the medaillon architecture: [Medaillon Architecure Documentation](doc/sections/medaillon_architecture/main.md)

## Documentation Sections

#### Getting Started:
- [AWS Setup](doc/sections/aws_setup/main.md)
- [Databricks Configuration](doc/sections/databricks_configuration/main.md)
- [Local Setup](doc/sections/local_setup/main.md)

#### Architecture and Main Components:
- [Medaillon Architecure](doc/sections/medaillon_architecture/main.md)
- [Architectural Components](doc/sections/main_components/main.md)
- [Datalake Meta Store](doc/sections/databricks_metastore/main.md)

#### Ingestion
- Spark Streaming
- Incremental Load
- [Schema Evolution](doc/sections/schema_evolution/main.md)

#### Data Transformations
- Supported data transformations
- Replay

#### Jobs and Orchestration
- [Databricks Workflows](doc/sections/databricks_workflows/main.md)
- Transfer Log table

#### Automated Testing
- How to create a new test

#### How to:
- [Creating a new pipeline](doc/sections/creating_pipelines/main.md)
- [Creating and running tests](doc/sections/automated_testing/main.md)
- [Delete duplicates in a delta table](doc/sections/debug_remove_dupes.md/main.md)
