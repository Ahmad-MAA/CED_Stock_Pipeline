---

# **Building an ETL Pipeline to Streamline Stock Analytics for Capital Edge**

## **Project Overview**
This project aims to build a robust, cloud-based ETL pipeline to automate the ingestion, transformation, and loading of financial data for Capital Edge. The pipeline ensures real-time, scalable, and accurate data analytics while supporting incremental data updates.

---

## **Rationale**
A streamlined ETL pipeline provides:
- **Automation**: Eliminates manual data collection processes.
- **Efficiency**: Reduces time and resources spent on data management.
- **Scalability**: Handles growing data volumes and additional clients as the company expands.
- **Data Accuracy and Timeliness**: Enables real-time access to reliable data for better decision-making.
- **Data Consistency**: Ensures uniform datasets across teams and departments.

---

## **Project Goals**
1. **Automate Data Collection**  
   Build a fully automated system that retrieves financial data from APIs without manual intervention.

2. **Enable Incremental Data Loading**  
   Optimize storage and processing by updating only new or modified data.

3. **Improve Data Accuracy**  
   Deliver real-time, accurate data for superior financial analysis.

4. **Scalability**  
   Develop a solution that adapts to increasing data volumes and user needs.

5. **Operational Efficiency**  
   Reduce data management overhead, allowing teams to focus on analysis and client service.

6. **Data Consistency**  
   Ensure all departments access the same, consistent data.

---

## **The Data**
- **Source**: Stock price data retrieved from APIs like Alpha Vantage.  
- **Format**: Extracted in JSON format and transformed into CSV.  
- **Frequency**: Daily updates to capture new market data.

---

## **Tech Stack**
- **API**: Data extraction from external financial APIs.  
- **Azure Blob Storage**: Data staging and storage.  
- **Google Cloud Platform (GCP)**: Cloud Composer for workflow automation.  
- **Apache Airflow**: Orchestrating the ETL pipeline.  
- **Python**: For scripting and automation.

---

## **Project Workflow**

### 1. **API Data Extraction**
Extract stock data for a list of symbols using an API key for authentication.  
**Input**: List of stock symbols.  
**Output**: A Pandas DataFrame containing the stock data.

---

### 2. **Data Transformation**
- Clean and deduplicate the extracted data.  
- Prepare it for incremental loading into the target storage.

---

### 3. **Data Staging in Cloud Storage**
- Temporarily stage transformed data in Azure Blob Storage.  
- Organize data into timestamped CSV files for easy retrieval and analysis.

---

### 4. **Incremental Data Loading**
- Retrieve previous datasets from Azure Blob Storage.  
- Combine with the latest data and remove duplicates.  
- Perform final cleaning steps (e.g., dropping null values).  
- Rename and upload the updated dataset to Azure with a timestamp.

---

### 5. **Workflow Automation with Airflow**
- **Local Testing**:  
  Create a virtual environment and install the necessary libraries:
  ```bash
  pip install pandas azure-storage-blob python-dotenv apache-airflow
  ```
- **Cloud Deployment**:  
  Scale the ETL process to Google Cloud Composer for full automation, with data loaded to Azure Blob Storage.

---

## **Key Features**
- **Incremental Loading**: Optimizes resource usage by only updating new or changed data.
- **Cloud-Native**: Utilizes GCP and Azure for scalability and reliability.
- **Automation**: Managed with Airflow to schedule, monitor, and maintain the pipeline.

---

This project is designed to provide Capital Edge with an efficient, scalable, and fully automated data pipeline to power real-time stock analytics.

---