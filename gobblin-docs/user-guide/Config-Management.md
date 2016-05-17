##Introduction
There are multiple challenges in dataset configuration management in the context of ETL data processing. ETL infrastructure employs multi-state processing flows to ingest and publish data on HDFS. Here are some examples types of datasets and types of processing:
* OLTP Snapshots: ingest, publishing, replication, retention management, compliance post-processing
* OLTP Increments: ingest, publishing, replication, roll-up, compaction, retention management
* Streaming data: ingest, publishing, roll-up of streaming data, retention management, compliance post-processing
* Opaque (derived) data: replication, retention management

We want to be able to customize the processing of each dataset like enabling/disabling certain types of processing, specific SLAs, access restrictions, retention policies, etc.

Design a backend and flexible client library for storing, managing and accessing configuration that can be used to customize the process of thousands of datasets across multiple systems/flows.



