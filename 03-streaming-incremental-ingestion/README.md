# Lab 3: Bronze Streaming & Incremental Ingestion

## Overview
This repository implements the Bronze ingestion layer for the **NASDAQ-100 Market Radar** pipeline on Databricks Unity Catalog using Spark Structured Streaming.

### Ingestion Streams
1. **Market News (Auto Loader)**: Ingests Finnhub JSON files incrementally into `raw_finnhub_news` with schema evolution (`cloudFiles.schemaEvolutionMode = "addNewColumns"`).
2. **Price Ticks (Azure Event Hubs)**: Streams real-time price tick events into `raw_nasdaq_ticks` using Kafka/AMQP protocol with ingestion audit metadata.

---

## Repository Structure

```text
├── 00_setup/
│   └── 00_valerii_env_setup.ipynb          # Catalog, schema, and volume initialization
├── 01_landing/
│   ├── 01a_landing_finnhub_news.ipynb      # Finnhub API fetcher (1,000 JSON files)
│   └── 01b_producer_eventhub.ipynb         # Event Hubs price tick producer
└── 02_bronze/
    ├── 02a_bronze_autoloader_news.ipynb     # Auto Loader stream -> raw_finnhub_news
    └── 02b_bronze_eventhub_ticks.ipynb      # Event Hubs stream -> raw_nasdaq_ticks
```

## Workflow Execution

The pipeline is orchestrated via Databricks Workflows:

* **Job Name**: `valerii_lab03_bronze_ingestion`
* **Job Run Link**: https://adb-7405604503619901.1.azuredatabricks.net/jobs/4074362321373/runs/1047946934475588?o=7405604503619901
* **Cost Optimization**: Both streams run with `.trigger(availableNow=True)` to process queued backlogs in micro-batches and terminate serverless compute automatically.
* **Fault Tolerance**: Offsets and state are managed under `_state/checkpoints/` in Unity Catalog volumes.

## Optional Additions

### 1. Exactly-Once vs. At-Least-Once Semantics
* **At-Least-Once Semantics**: Guarantees that every event from the source is processed at least once. If a failure occurs before a micro-batch offset is committed, the engine reprocesses the batch on restart, which can introduce duplicate records in non-idempotent sinks.
* **Exactly-Once Semantics**: Guarantees that each event affects the target sink exactly once, with no duplicates. Achieved by combining a replayable source (Auto Loader or Event Hubs) with an idempotent target (Delta Lake ACID transactions).

### 2. Checkpointing & Fault Tolerance
* **Checkpointing**: Spark maintains stream state by writing progress tracking (offsets, metadata) to a persistent directory (`checkpointLocation`) in Unity Catalog Volumes.
* **Fault Tolerance**: If a cluster fails mid-stream, Spark recovers state from the checkpoint directory on restart, resuming execution from the last uncommitted offset without data loss or duplicates.