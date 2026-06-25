# Project Overview

## Project Name

**Construction Cost Monitoring System - Brazil**

## Created At

May 25, 2026

## Goal

Build a data system that collects, processes, and provides information about construction costs in Brazil over time.
The system should help users understand trends, changes, and patterns in construction prices.

---

# Project Scope

## Data Collection

* Download official data from SINAPI (IBGE)
* Download and process CUB/m² data (Brazil-wide, from Sinduscon)
* Generate simulated data for construction materials

---

# Data Sources

## SINAPI (Construction Cost Data)

* Source: IBGE (Brazilian Institute of Geography and Statistics)
* Description: Official data on construction costs in Brazil

Links:

* https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9270-sistema-nacional-de-pesquisa-de-custos-e-indices-da-construcao-civil.html
* https://www.caixa.gov.br/site/Paginas/downloads.aspx#categoria_888

---

## CUB/m² (Basic Unit Cost of Construction)

* Source: CBIC (Brazilian Chamber of the Construction Industry) + Sinduscon (state-level)
* Description: A key indicator used as a reference for construction costs per square meter in Brazil
* Notes:

  * Published monthly by each state (Sinduscon)
  * Aggregated into a national average (CUB Brazil)
  * Widely used for budgeting and cost estimation

Link:

* https://www.cub.org.br/cub-m2-brasil

---

# System Architecture

## Overview

The system follows a simple data pipeline:

**Data Sources → Extraction → Processing → Storage → Delivery → Visualization**

---

## Data Extraction

### SINAPI

* Data is fetched using an API endpoint that lists all available ZIP files
* Endpoint:
  https://www.caixa.gov.br/_api/web/lists/Downloads/Items
* ZIP files are downloaded and processed in runtime
* Excel files are read directly without manual extraction

### CUB/m²

* Data is fetched from state-level endpoints using HTTP POST requests
* Endpoint pattern:
  http://www.cub.org.br/cub-m2-estadual/{UF}
* Data is extracted from PDF files and converted into tables

---

## Data Processing

* Two specialized modules are responsible for:

  * Fetching raw data
  * Converting data into tabular format
* The main tool used for table operations is **Polars**
* All data is transformed into structured schemas (protocols/models)
* This ensures:

  * Consistent column names
  * Fixed data types
  * Standardized structure across datasets

---

## Data Storage

* A single main process orchestrates all data operations
* Data is stored in **Cloudflare object storage**
* This avoids early infrastructure costs and simplifies deployment

### Storage Strategy

* Each dataset is stored in a **Delta Lake-like folder structure**
* Data is written using an **insert-only** approach:

  * No updates or deletions
  * Only new records are appended
* Each record includes a **timestamp** field

  * Used to identify the most recent data

---

## Data Delivery

* The latest version of each dataset is selected using the timestamp column
* Filtered data is exported as:

  * CSV files
  * Compressed using GZip for efficiency
* Files are stored in the same Cloudflare bucket

---

## Visualization

* The dashboard is developed using **Power BI**
* Cloudflare provides a public endpoint to access stored files
* The Power BI dashboard reads the compressed CSV files directly
* This allows real-time visualization without a traditional database layer

---

## Orchestration

* The entire pipeline runs inside a single main process
* This process is wrapped in an orchestration flow using **Prefect Cloud**
* The pipeline runs automatically:

  * Once per month
  * At the end of each month

---

# Project Structure

## Core Modules

* **sinapi_api.py**
  Reads raw SINAPI data and produces dataframes in different formats and states

* **cub_api.py**
  Reads raw CUB data from PDFs and produces structured dataframes

* **data_models**
  Transforms raw data into strict schemas

  * Enforces column names
  * Enforces data types
  * Not resilient to format changes (strict validation)

* **core.py**
  Implements the main pipeline logic

  * Reads environment configuration
  * Orchestrates data extraction, processing, and storage

* **main.py**
  Top-level entry point

  * Wraps the pipeline into a Prefect flow
  * Enables remote execution

* **application (package)**
  Custom internal package

  * Provides shared utilities
  * Encapsulates reusable logic and abstractions

---

## Environment Configuration

* **.env**
  Global environment file

  * Stores sensitive credentials
  * Can be reused across multiple flows

* **.env.local**
  Local environment file

  * Flow-specific configuration
  * Controls execution behavior

---

# Architecture Decisions

## Why Cloudflare Object Storage

* Lower cost compared to traditional web databases
* No significant data transfer costs (generous limits)
* Scales from small (10 GB) to larger storage (up to ~100 GB) at low cost
* Simple and sufficient for analytical workloads

## Why CSV + GZip Instead of Parquet

* Power BI cannot reliably read Parquet files directly from web sources
* CSV is universally supported
* GZip compression reduces storage usage and transfer size

## Why Polars

* Supports lazy execution
* Efficient for large-scale data transformations
* Enables processing data without loading everything into memory

## Why Insert-Only Strategy

* Safer than modifying or deleting existing data
* Preserves full historical records
* Enables reproducibility and auditability
* Latest data can always be derived using timestamps

---

# Known Limitations

* The system depends on external data formats (SINAPI and CUB)

* If source templates (Excel or PDF) change, extraction may break

* There is no dynamic extractor selection system

* The pipeline cannot adapt automatically to different file formats or layouts

* The `data_models` module is strict by design

* If data does not match expected schema:

  * Processing fails
  * No fallback or partial parsing is performed

* PDF parsing (CUB) is inherently fragile

* Small layout changes can break data extraction

---

# Final Product Requirements

The project is considered complete only when all items below are done:

* Functional data pipeline (end-to-end)
* Storage system populated with processed data
* Dashboard ready for visualization
* Code published on GitHub
* Clear and complete documentation
