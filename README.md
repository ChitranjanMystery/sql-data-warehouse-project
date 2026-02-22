# 🏗️ SQL Data Warehouse Project

## 📌 Project Overview

This project demonstrates the design and implementation of a **modern Data Warehouse** using **Microsoft SQL Server** to consolidate sales data from multiple source systems.  

The objective is to enable analytical reporting and support data-driven decision-making through a structured and scalable architecture.

---

## 🎯 Project Specifications

### 🔹 Data Sources
- ERP system (CSV files)
- CRM system (CSV files)

### 🔹 Data Quality
- Cleanse and resolve data quality issues before analysis.

### 🔹 Integration
- Combine both source systems into a single user-friendly analytical data model.

### 🔹 Scope
- Focus only on the latest dataset.
- Historical data tracking (historization) is not required.

### 🔹 Documentation
- Provide clear documentation of:
  - Data model
  - Data flow
  - Business logic
  - Metadata structure

---

# 🏛️ Choosing a Data Management Approach

## Data Architecture Options

1. Data Warehouse  
2. Data Lake  
3. Data Lakehouse  
4. Data Mesh  

For this project, we implemented a **Data Warehouse using Medallion Architecture**.

---

# 🏗️ Data Warehouse Architecture

## Approaches to Building a Data Warehouse

| Approach | Flow |
|----------|------|
| **Inmon** | Raw → Staging → EDW (3NF) → Data Marts → Visualization |
| **Kimball** | Raw → Staging → Data Marts → Visualization |
| **Data Vault** | Raw → Staging → Raw Vault → Business Vault → Data Marts → Visualization |
| **Medallion** | Raw → Bronze → Silver → Gold |

This project follows the **Medallion Architecture**.

---

# 🥉 Bronze Layer

### Definition
Raw, unprocessed data loaded as-is from source systems.

### Objective
Ensure traceability and debugging capability.

### Characteristics

- **Object Type:** Tables  
- **Load Method:** Full Load (Truncate & Insert)  
- **Transformations:** None  
- **Data Modeling:** None  
- **Target Audience:** Data Engineers  

---

## Bronze Layer Process

### 🔎 Analysis
- Interview source system experts
- Understand business ownership
- Review system documentation
- Identify data catalog and schema

### ⚙ Architecture Review
- Storage type (SQL Server, Oracle, AWS, Azure, etc.)
- Integration methods (API, file extract, DB connection)

### 📥 Extraction & Loading
- Full vs Incremental strategy
- Data scope & size expectations
- Performance considerations
- Authentication & authorization (VPN, SSH, tokens, etc.)

### 🧪 Validation
- Data completeness checks
- Schema validation

### 📘 Documentation
- Version control using Git
- Data ingestion documentation

---

# 🥈 Silver Layer

### Definition
Cleaned and standardized data prepared for analysis.

### Objective
Transform raw data into structured, analytics-ready datasets.

### Characteristics

- **Object Type:** Tables  
- **Load Method:** Full Load  
- **Transformations:**
  - Data Cleaning
  - Standardization
  - Normalization
  - Derived Columns
  - Data Enrichment
- **Target Audience:** Data Engineers & Analysts  

---

## Silver Layer Process

### 🔎 Analysis
- Explore and understand raw datasets

### ⚙ Transformation
- Validate Bronze quality
- Apply business rules
- Insert transformed data into Silver tables

### 🧪 Validation
- Data correctness checks
- Reconciliation testing

### 📘 Documentation
- Data flow documentation
- Integration mapping
- Version control in Git

---

## 📌 Metadata Columns

Additional columns added by data engineers:

- `create_date` → Record load timestamp  
- `update_date` → Last update timestamp  
- `source_system` → Originating system  
- `file_location` → Source file path  

---

# 🥇 Gold Layer

### Definition
Business-ready data optimized for reporting and analytics.

### Objective
Provide high-quality datasets for business users and analysts.

### Characteristics

- **Object Type:** Views  
- **Load Method:** None (derived from Silver)  
- **Transformations:**
  - Data Integration
  - Aggregation
  - Business Logic
- **Data Modeling:**
  - Star Schema
  - Aggregated Objects
  - Flat Tables
- **Target Audience:** Analysts & Business Users  

---

## Gold Layer Process

### 🔎 Business Analysis
- Identify business objects
- Define KPIs & metrics

### ⚙ Integration
- Build fact and dimension tables
- Apply business rules
- Rename columns to business-friendly names

### 🧪 Validation
- KPI validation
- Aggregation accuracy checks

### 📘 Documentation
- Data Model
- Data Catalog
- Data Flow diagrams

---

# 📊 Data Modeling

## Types of Data Models

1. **Conceptual Model** – High-level business view  
2. **Logical Model** – Blueprint of structure  
3. **Physical Model** – Implementation in SQL Server  

---

## Schema Design

### ⭐ Star Schema
- Simple and easy to understand
- Large dimension tables
- Optimized for reporting

### ❄ Snowflake Schema
- More complex
- Suitable for large datasets

---

## Dimensions vs Facts

### 📘 Dimensions
Descriptive attributes providing context:
- Who?
- What?
- Where?

### 📈 Facts
Quantitative metrics:
- How many?
- How much?
- Dates, IDs, numeric measures

---

# 🧠 Core Principles

- Separation of Concerns
- Optimized for Reporting
- Flexible Design
- Easy to Understand

---

# 🏷️ Naming Conventions

## General Rules

- Use **snake_case**
- Use lowercase letters
- Use English language only
- Avoid SQL reserved keywords

## Naming Styles

- camelCase  
- kebab-case  
- snake_case  
- SCREAMING_SNAKE_CASE  

---

# 🚀 Final Outcome

This project demonstrates:

- End-to-end Data Warehouse development
- ETL pipeline implementation
- Data modeling best practices
- Analytical data transformation
- Professional documentation & version control

---

## 👨‍💻 Author

**Chitranjan Thakur**  
Data Analyst | SQL | Data Engineering | Analytics
