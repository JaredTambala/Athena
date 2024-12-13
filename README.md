# Athena

## Overview

An implementation of a modern, scalable and modular Data Lakehouse architecture designed for data ingestion, analysis and modelling at scale using purely open-source technologies. 

![image](/docs/images/Athena.png)


This platform consists of the following major components:

- <b>Apache Airflow</b>: Workflow/ETL orchestration.
- <b>MinIO</b>: Object Storage used for Lakehouse and MLFlow Artifact Storage.
- <b>Apache Hive</b>: Metastore used for creating tables from Parquet data. <i>Subject to change</i>
- <b>Apache Spark</b>: MPP engine for data processing, streaming and ML computations.
- <b>Trino</b>: MPP engine optimised for data analytics/exploration.
- <b>MLFlow</b> Machine learning model development, registration, tracking and deployment.

This platform has been designed with the use case of financial time series modelling and predictive analytics in mind, but otherwise serves as a demonstration of aspects of my Data Engineering skillset. 

## Deployment

The current deployment mechanism for this platform is via Docker Compose. In <b><i>/docker/build/</i></b>, <b><i>docker-compose.yaml</b></i> refers to several Docker Compose files, each of which builds a self-contained aspect of the platform. These have been designed to be modular - removing a module is as simple as commenting it out in docker-compose.yaml, and the other components should still work together effectively.




