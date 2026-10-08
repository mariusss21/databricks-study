# 1.1 Lecture - What is Apache Spark

> #### *A unified large-scale data processing and analytics framework*

This lecture introduces Apache Spark: what it is, where it came from, and what makes up the Spark 4 component stack.

## Learning Objectives

**By the end of this lecture, you will be able to:**

* Describe Apache Spark and the workloads it supports
* Summarize the history of the Spark project and its relationship to Databricks
* Identify the major components of the Spark 4 stack and how they relate to the DataFrame API

## A. What is Apache Spark?

Apache Spark is an open-source, distributed, unified analytics engine for fast, large-scale data processing. Click any card to flip it and reveal more detail.

Unified

Native support for SQL, streaming data, machine learning, and graph processing through a consistent API.

Click to flip

Unified API

* The same DataFrame code can power a batch ETL job, a streaming pipeline, or feature preparation for machine learning, so teams learn one API rather than four separate systems.
* SQL, Python, Scala, Java, and R interfaces all compile down to the same execution engine, producing the same query plans regardless of language.

Distributed

Work is split across many machines, allowing datasets far larger than any single machine could hold.

Click to flip

Distributed Execution

* Spark splits data into partitions and processes them in parallel across a cluster of machines, scaling horizontally by adding nodes.
* Fault tolerance is built in: if a node fails, Spark recomputes only the lost partitions using the lineage recorded in the query plan.

Fast

High performance through in-memory computation, making it significantly faster than disk-based processing frameworks.

Click to flip

In-Memory Performance

* Intermediate results are held in memory wherever possible, avoiding the repeated disk writes that limit older frameworks such as Hadoop MapReduce.
* On Databricks, the Photon engine further accelerates execution by processing data in vectorized batches.

Open

Operates seamlessly with diverse data sources including cloud storage (AWS S3, Azure, Google Cloud), local files, and databases.

Click to flip

Open and Interoperable

* Built-in connectors read and write cloud object storage (Amazon S3, Azure Data Lake Storage, Google Cloud Storage), local and distributed file systems, and JDBC databases.
* Open file and table formats such as Parquet, JSON, CSV, Delta Lake, and Apache Iceberg avoid lock-in to any single storage system.

## B. A Brief History of Spark and Databricks

Spark began as a research project at UC Berkeley and has grown into one of the most active open-source projects in data. Databricks was founded by the original creators of Spark. Click the bar below to reveal the timeline, grouped into four eras.

▶
**Spark and Databricks timeline**

Spark and Databricks Milestones

Origins
2009–2013

Apache Project
2014–2016

Lakehouse
2018–2021

Spark 4
2023–2025
›
›
›

2009
Originates at UC Berkeley
AMPLab

2010
Initial Spark release

2013
Databricks founded;
Spark to Apache

2014
Apache top-level project

2015
DataFrame API released

2016
Spark 2.0 and
Structured Streaming

2018
Delta Lake introduced

2020
Spark 3.0 and Adaptive
Query Execution

2021
Unity Catalog announced

2023
Spark 3.4 and
Spark Connect

2025
Spark 4.0 released

  

▶
**Expand for More Details**

From Research Project to Industry Standard

* Spark was created at UC Berkeley's AMPLab in 2009 to address the performance limits of Hadoop MapReduce, particularly for iterative workloads such as machine learning.
* The project was donated to the Apache Software Foundation in 2013 and became a top-level project in 2014, the same period in which the original creators founded Databricks.
* The DataFrame API (2015) and the unified `SparkSession` entry point in Spark 2.0 (2016) shifted Spark development from low-level RDD code to declarative, optimizer-friendly operations.

The Spark 4 Era

* Spark 3.x introduced Adaptive Query Execution (3.0) and Spark Connect (3.4), a client-server protocol that decouples the client application from the cluster.
* Spark 4.0, released in 2025, made ANSI SQL mode the default, added the `VARIANT` data type for semi-structured data, and promoted Spark Connect to a first-class architecture. Databricks Runtime 17.x is built on Spark 4.0.

## C. The Spark Component Stack

Spark is organized in layers. Specialized libraries sit on top of the DataFrame API, which in turn runs on Spark Core. Click the bar below to reveal the diagram.

▶
**Spark component stack diagram**

Structured Streaming
Real-time processing
with DataFrame semantics

MLlib
Machine learning library

Spark SQL
SQL interface for
structured data

pandas API on Spark
pandas-compatible
DataFrame interface

GraphX
Graph processing library

DataFrame API
High-level API for structured data operations

Spark Core (RDD API)

  

▶
**Expand for More Details**

The Libraries

* **Structured Streaming** processes unbounded data using the same DataFrame operations you use for batch, so a streaming pipeline looks almost identical to a batch one.
* **MLlib** provides distributed implementations of common machine learning algorithms and feature transformers.
* **Spark SQL** exposes the engine through standard SQL, and shares the optimizer and execution engine with the DataFrame API.
* **pandas API on Spark** offers a pandas-compatible interface for Python users, executing pandas-style code at cluster scale.
* **GraphX** supports graph algorithms; in practice most graph work in Python uses the GraphFrames package built on DataFrames.

The Foundation Layers

* The **DataFrame API** is the primary programming interface in modern Spark. It is declarative: you describe the result you want, and the Catalyst optimizer determines how to compute it.
* **Spark Core** implements scheduling, memory management, fault recovery, and the low-level RDD (Resilient Distributed Dataset) abstraction. In Spark 4 you rarely program against RDDs directly, and the RDD API is not available over Spark Connect, but everything still executes on this layer.
* The R language binding (SparkR) is deprecated as of Spark 4.0; R users are directed to the community-supported sparklyr package.

## D. What is New in Spark 4

This course is based on **Apache Spark 4**, the version that ships with **Databricks Runtime 17.x**. The table below summarizes the headline changes you will encounter in this course.

| Feature | What it Means |
| --- | --- |
| ANSI SQL mode by default | Invalid casts and arithmetic overflows now raise errors instead of silently returning `NULL`. Use `try_cast()` for permissive conversions. |
| Spark Connect | A client-server architecture that decouples your application from the cluster using a thin client and the gRPC protocol. |
| `VARIANT` data type | A native type for semi-structured data (such as JSON) that is faster and more flexible than storing JSON as strings. |
| Native plotting | Plot directly from a PySpark DataFrame with `df.plot` (backed by plotly), without converting to pandas first. |
| SQL scripting | Procedural SQL with `BEGIN ... END` blocks, variables, conditionals, and loops. The course setup notebooks use this. |
| Arrow-optimized Python UDFs | Python user defined functions can use Apache Arrow for serialization, reducing the Python-JVM overhead. |

You will meet ANSI mode and `try_cast()` in the ETL demo, native plotting in Section 4, and SQL scripting in the course setup notebooks.

## Summary

* Apache Spark is an open-source, distributed, unified analytics engine for large-scale data processing
* Spark originated at UC Berkeley in 2009; Databricks was founded by its creators in 2013
* Specialized libraries (Structured Streaming, MLlib, Spark SQL, pandas API on Spark) sit on the DataFrame API, which runs on Spark Core
* Spark 4 brings ANSI SQL mode by default, Spark Connect, the `VARIANT` type, native plotting, and SQL scripting
