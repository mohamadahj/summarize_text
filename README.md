# 📘 AI-Powered Text Summarization Pipeline
Azure Databricks, Azure OpenAI, Azure Blob Storage & Azure Function Apps

## 🗒️ Overview
This project implements a robust and scalable text summarization pipeline leveraging the Databricks Medallion Architecture (Bronze, Silver, Gold). It integrates Azure OpenAI (GPT-4) to generate concise summaries from unstructured text data. The summarized data is securely stored in Azure Blob Storage, exposed via an Azure Function API endpoint, and consumed back within Databricks.

## 📚 Pipeline Structure
### 🟤 Bronze Layer: Raw Data Ingestion
 - Purpose:
* Initial ingestion of raw text data from CSV into Spark DataFrames.
 -- Implementation:
    Reads CSV file into Spark DataFrame.
  No transformation; raw data storage only.

### 🥈 Silver Layer: Data Cleaning and Preprocessing
 - Purpose:
 -- Clean and structure raw data for reliable downstream processing.
 -- Transformations applied:
 --- Duplicate removal
 --- Text normalization (lowercasing, special character removal, whitespace trimming)
 --- Conversion of ID column to integer type
 --- Handling null or empty values

### 🥇 Gold Layer: AI-Powered Text Summarization (Azure OpenAI GPT-4)
 - Purpose:
 -- Generate concise, high-quality summaries from cleaned text using Azure OpenAI.
 -- Implementation highlights:
 --- Robust error handling with exponential backoff
 --- Batch processing using mapInPandas for scalability
 --- Secure API key retrieval from Databricks Secrets

## 📦 Saving Data to Azure Blob Storage
 - Purpose:
 -- Persist the summarized data securely for further use.
 -- Details:
 --- Data stored in Parquet format for efficient analytics.

## 🌐 Azure Function API for Data Retrieval
 - Purpose:
 -- Provide a simple, scalable API endpoint to fetch summarized data.
 -- Technology:
 --- Python HTTP-triggered Azure Function returning structured JSON.
## 🔄 Consuming Azure Function API from Databricks
 - Purpose:
 -- Demonstrate retrieval of summarized data back into Databricks.
## 🚧 Potential Improvements
 - Implement detailed monitoring (Azure Monitor).
 - Automate deployments (CI/CD) with Azure DevOps or GitHub Actions.
 - Optimize parallelization and partitioning strategies for larger datasets.
## ✅ Conclusion
This end-to-end pipeline clearly demonstrates scalable data ingestion, cleaning, summarization with Azure OpenAI GPT-4, secure data storage, and retrieval via APIs, making it robust and ready for production analytics workloads.
