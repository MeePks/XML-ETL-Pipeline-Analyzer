# XML-ETL-Pipeline-Analyzer

A Python-based tool to parse, analyze, and process XML files used in ETL (Extract, Transform, Load) pipelines. This script extracts metadata from transformation (`tf.xml`) and mapping (`map.xml`) files, standardizes it, and prepares the data for storage and analysis in a relational database (e.g., SQL Server).  

---

## Features

- **XML Parsing**: Extracts detailed metadata from transformation and mapping XML files.
- **Data Transformation**: Standardizes and enriches metadata for structured analysis.
- **Database Integration**: Inserts processed data into SQL Server tables for reporting and auditing.

---

## Use Case

This project is particularly useful for ETL developers working with XML-based metadata files to:
1. Extract transformation and mapping details.
2. Normalize metadata for downstream processing.
3. Automate the logging and auditing of ETL pipelines.

---

## Prerequisites

- Python 3.8 or higher
- SQL Server (optional, for database integration)
- Required Python libraries:
  - `xml.etree.ElementTree`
  - `pyodbc`
