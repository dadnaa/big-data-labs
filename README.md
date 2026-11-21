# Big Data Labs
This repository is a practical progression through distributed data processing, analytics, machine learning, graph computation, and streaming systems. The labs move from low-level Hadoop MapReduce to higher-level Spark APIs, then extend into GraphFrames and Kafka/ksqlDB.
The work is organized as solution folders for six practical assignments:
1. Hadoop and HDFS
2. Spark RDD
3. Spark SQL
4. Spark MLlib
5. GraphFrames
6. Kafka and ksqlDB
Together, they form a foundation for building data-intensive applications that can ingest, transform, analyze, learn from, and continuously process large datasets.
## Learning Objectives
The main objective is to understand not only how to write data-processing code, but also how to choose an appropriate abstraction for a problem:
- use MapReduce for explicit distributed batch computation;
- use RDDs for resilient, functional transformations;
- use DataFrames and SQL for structured, optimized analytics;
- use MLlib pipelines for repeatable machine-learning workflows;
- use graph abstractions for relationships and path-based analysis;
- use Kafka and ksqlDB for event-driven, continuously updated systems.
The labs also emphasize practical concerns such as schema inference, missing values, data types, reproducibility, local testing, cluster configuration, and the difference between code that is written and code that has been validated in a real runtime environment.
## Lab Summary
### TP1 — Hadoop, HDFS, and MapReduce
`TP1_Hadoop_Solution/` builds a Java Maven WordCount application and runs it against a Dockerized Hadoop environment.
The lab covers:
- creating a small Hadoop cluster with Docker;
- starting HDFS and interacting with files using Hadoop commands;
- compiling a Maven MapReduce JAR;
- implementing a mapper, reducer, and driver;
- uploading input data to HDFS;
- executing a distributed word-count job;
- reading reducer output from HDFS.
**Core insight:** MapReduce makes distributed computation explicit. The mapper emits intermediate key/value pairs, the reducer aggregates values by key, and the driver defines the execution graph and serialization types.
### TP2 — Spark RDD and Functional Transformations
`TP2_Spark_RDD_Solution/` analyzes tree data with PySpark RDDs.
The lab demonstrates:
- loading distributed text data with `textFile`;
- handling headers and detecting delimiters;
- parsing records by column name;
- calculating an average with `map`, `reduce`, and `count`;
- sorting key/value pairs to identify the tallest tree;
- aggregating tree counts with `reduceByKey`;
- running the same logic locally or on a Dockerized Spark cluster.
**Core insight:** RDDs provide a resilient distributed collection model and make transformations such as mapping, filtering, sorting, and reduction visible. They are useful for understanding Spark's execution model, even when higher-level APIs are preferable for structured data.
### TP3 — Spark SQL and DataFrame Analytics
`TP3_SparkSQL_Solution/` processes an n-gram dataset using both Spark SQL and the DataFrame API.
The lab covers:
- creating a typed DataFrame with an explicit schema;
- detecting delimiter and header formats;
- registering a temporary SQL view;
- filtering, grouping, joining, ordering, and aggregating records;
- finding yearly maxima and recurring n-grams;
- using string splitting and distinct counts;
- validating that SQL and DataFrame implementations produce equivalent results.
Eight analytical questions are implemented twice: once with SQL and once with the DataFrame API.
**Core insight:** Structured APIs improve readability and optimization opportunities. Expressing the same computation in SQL and DataFrame operations also provides a useful correctness check and helps translate between analyst-oriented and programmatic workflows.
### TP4 — Spark MLlib and KMeans
`TP4_MLlib_Solution/` builds a clustering pipeline over retail transaction data.
The lab demonstrates:
- loading multiple CSV files with schema inference;
- inspecting a distributed DataFrame schema;
- handling missing numeric values;
- deriving the day of week from transaction timestamps;
- splitting data temporally into training and test sets;
- encoding categorical values with `StringIndexer` and `OneHotEncoder`;
- assembling numerical and categorical features;
- fitting KMeans with `Pipeline`;
- generating test predictions;
- evaluating clustering with the silhouette metric.
**Core insight:** Machine learning on distributed data depends on a disciplined feature pipeline. Encoding categories without introducing false numeric ordering, fitting transformations on training data, and evaluating predictions consistently are as important as selecting the clustering algorithm.
### TP5 — GraphFrames and Network Analysis
`TP5_GraphFrames_Solution/` models bike stations and trips as a directed graph.
The lab covers:
- representing stations as vertices and trips as edges;
- constructing a `GraphFrame`;
- counting routes between station pairs;
- identifying high in-degree stations;
- filtering graph edges into a subgraph;
- finding triangles and multi-hop paths;
- using GraphFrame motif queries;
- reproducing motif analysis with DataFrame self-joins as a fallback.
**Core insight:** Graph modeling makes relationships first-class. The same data can be analyzed with specialized graph pattern syntax or relational joins, which is valuable when graph dependencies are unavailable or when a platform requires a SQL/DataFrame-compatible fallback.
### TP6 — Kafka and ksqlDB Streaming
`TP6_Kafka_ksqlDB_Solution/` defines a Kafka and ksqlDB pipeline for monitoring Netflix production changes.
The lab introduces:
- a Docker Compose environment with ZooKeeper, Kafka, and ksqlDB;
- Kafka topics represented as a table of current title state and a stream of immutable production events;
- nested `STRUCT` values for season metadata;
- derived streams for season-length changes;
- stream-table joins to enrich events with title names;
- tumbling-window aggregation;
- push queries using `EMIT CHANGES`;
- event-time configuration and timestamp parsing.
**Core insight:** Streaming systems distinguish between changing state and event history. Correctly choosing `TABLE` versus `STREAM`, preserving event time, and composing derived streams are central to real-time analytics.
## Skills Gained
### Distributed data engineering
- Hadoop cluster and HDFS fundamentals
- MapReduce program structure
- Spark execution concepts
- RDD transformations and actions
- DataFrame and SQL-based distributed processing
- Docker-based environment setup
- local-versus-cluster execution workflows
### Data preparation and quality
- schema definition and inference
- delimiter and header detection
- parsing heterogeneous input records
- missing-value handling
- timestamp conversion and temporal partitioning
- numeric and categorical feature preparation
- explicit validation of alternative implementations
### Analytics and machine learning
- aggregation and ranking
- joins, grouping, filtering, and distinct counting
- feature pipelines
- categorical encoding
- KMeans clustering
- silhouette-based evaluation
- interpretation of graph degree and motif patterns
### Graph and streaming systems
- vertex and edge data modeling
- graph traversal and motif queries
- relational fallbacks for graph operations
- Kafka topic and event concepts
- ksqlDB streams, tables, joins, windows, and push queries
- nested event schemas and event-time processing
### Engineering practice
- organizing reproducible lab environments
- documenting assumptions and runtime constraints
- creating small fixture datasets for local testing
- separating verified behavior from unverified deployment behavior
- designing fallback paths when optional infrastructure is unavailable
## Validation Status
The repository includes local verification paths for the Hadoop, Spark RDD, Spark SQL, and Spark MLlib work using sample or synthetic data. Spark SQL results are explicitly compared between SQL and DataFrame implementations, and the GraphFrames lab includes an independent DataFrame self-join fallback.
Some results depend on infrastructure or datasets that are not included in the repository:
- the real Hadoop cluster and Docker deployment require a compatible local environment;
- the original tree and n-gram datasets are not included;
- the MLlib and graph labs use small fictitious datasets for local tests;
- the GraphFrames Java package must be downloaded with a Spark-compatible version;
- the Kafka/ksqlDB lab has not been executed in the documented environment and may require version-specific syntax adjustments.
These limitations are part of the learning record: a reproducible data solution should state which parts were executed, which inputs were synthetic, and which deployment assumptions remain to be tested.
## Repository Structure
```text
big-data-labs/
├── TP1_Hadoop_Solution/
├── TP2_Spark_RDD_Solution/
├── TP3_SparkSQL_Solution/
├── TP4_MLlib_Solution/
├── TP5_GraphFrames_Solution/
└── TP6_Kafka_ksqlDB_Solution/
```
Each lab contains its own README, source code or notebook, sample data where available, and execution instructions.
## Future Open Doors
These labs provide a starting point for larger engineering and research projects:
### Build a complete lakehouse pipeline
Combine Kafka ingestion, Spark Structured Streaming, schema management, and columnar storage such as Parquet or Delta Lake. Add checkpointing, replay, partitioning, and data-quality rules so the pipeline can recover from failure and reproduce historical results.
### Move from toy data to production-scale datasets
Replace the fixtures with public mobility, retail, text, climate, or public-administration datasets. Measure partitioning, shuffle volume, execution time, memory use, and scalability as data volume increases.
### Develop real-time machine learning
Use streaming features from Kafka, apply trained MLlib models to incoming events, and monitor prediction drift, feature quality, and model performance over time.
### Extend graph intelligence
Apply the graph workflow to mobility networks, recommendation systems, fraud detection, supply chains, or knowledge graphs. Add connected components, PageRank, community detection, shortest paths, and temporal graph analysis.
### Improve data governance and reliability
Introduce schema registries, data contracts, lineage tracking, automated data-quality checks, unit and integration tests, structured logging, and reproducible environment definitions.
### Productionize the infrastructure
Move from Docker demonstrations to Kubernetes or managed cloud services. Add CI/CD, observability, secrets management, access control, resource quotas, and automated deployment validation.
### Explore research questions
- How do different partitioning strategies affect Spark shuffle and graph workloads?
- When does an RDD implementation become less efficient than a DataFrame plan?
- How stable are KMeans clusters under changing time windows and feature scaling?
- How do streaming window sizes affect latency, completeness, and business interpretation?
- Which graph representation best captures repeated trips, station capacity, and temporal behavior?
- How can data provenance and uncertainty be communicated alongside analytical results?
## Closing Perspective
The central lesson of this repository is that big-data engineering is a progression of trade-offs. Low-level distributed primitives explain how computation works; structured APIs improve maintainability and optimization; machine-learning pipelines turn data into models; graph systems expose relationships; and streaming platforms make analysis continuous.
The next step is to connect these separate capabilities into one tested, observable, and deployable data platform.

