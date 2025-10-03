# ETL_finance_data_airflow

[![Diagram(diagram.png)]

[![Python](https://img.shields.io/badge/Python-3.9-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-orange)](https://airflow.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-EC2%20%7C%20S3-yellow)](https://aws.amazon.com/)
[![YFinance](https://img.shields.io/badge/YFinance-API-green)](https://pypi.org/project/yfinance/)

---

## **Project Overview**

**ETL_finance_data_airflow** is an end-to-end **ETL pipeline** built using **Apache Airflow** on an **AWS EC2 instance**. The workflow automatically fetches financial data from the **Yahoo Finance API (yfinance)**, transforms it, and loads it into an **AWS S3 bucket** in CSV format.  

**Key Highlights:**

- Fully automated **Airflow DAG** for scheduling and orchestration  
- Cloud-based storage using **AWS S3** for scalable and persistent data  
- Practical use of **Python, Airflow, and AWS services** for financial data pipelines  
- Modular and reusable design to integrate additional data sources  

---

## **Architecture Diagram**

```mermaid
flowchart TD
    A[Airflow Scheduler on EC2] --> B[DAG: ETL_finance_data]
    B --> C[Task 1: Fetch data from yfinance API]
    C --> D[Task 2: Transform data]
    D --> E[Task 3: Load CSV to S3]
    E --> F[AWS S3 Bucket]
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style C fill:#aff,stroke:#333,stroke-width:1px
    style D fill:#afa,stroke:#333,stroke-width:1px
    style E fill:#faa,stroke:#333,stroke-width:1px
    style F fill:#ffb,stroke:#333,stroke-width:2px
