# PySpark Big Data Analytics: Automotive Sales & Academic Text Classification

This repository showcases a comprehensive PySpark project focused on data engineering and analytical processing of both unstructured text and nested JSON datasets.

## 🚀 Project Overview

The project addresses two distinct big data challenges using Spark's distributed computing capabilities:

### 1. Purchase History & Frequency Analysis
**Objective:** Analyze raw sales logs to identify market trends and brand popularity.
* **Data Processing:** Processed unstructured log entries from `car_sales.txt` (e.g., `Ford - - 2015 SUV Auto Black 26`).
* **Operations:** Leveraged RDD transformations to extract car brands and performed a `countByKey` operation to aggregate sales.
* **Key Insight:** Identified the **Top 5** most frequent car brands: **Chevrolet (177), Dodge (173), Ford (167), Mitsubishi (146), and Volkswagen (131)**.

### 2. Academic Paper Classification & Text Mining
**Objective:** Process a complex JSON dataset of research papers and automatically classify sections based on headers.
* **Schema Handling:** Parsed multi-line JSON structures containing nested arrays for section names and sentences.
* **Automated Labeling:** Implemented a classification system using a predefined `KEYWORDS` dictionary to categorize sections into:
    * **Introduction (i)**
    * **Method (m)**
    * **Result (r)**
    * **Discussion (d)**
    * **Conclusion (c)**
* **Data Engineering Workflow:** * Used `explode` to flatten nested arrays into relational rows for processing.
    * Developed a **PySpark UDF** (User-Defined Function) to execute exact keyword matching logic across section headers.
    * Applied text refinement filters to include only high-quality sections with a word count greater than **50**.
* **Persistence:** Exported the final cleaned and sorted dataset into a structured JSON format.

## 🛠️ Technical Stack

* **Core Framework:** PySpark (Spark SQL, DataFrame API, & SparkContext).
* **Language:** Python.
* **Supporting Libraries:** Pandas (for DataFrame bridging), JSON.
* **Environment:** Google Colab / Jupyter Notebook.

## 📂 File Structure

* `assignment_1_GUI_Shengqi.ipynb`: The complete PySpark source code and execution results.
* `car_sales.txt`: Source file containing raw automotive sales logs.
* `assignment.text.20.json`: Source dataset containing academic paper metadata and text.

## ⚙️ How to Run

1.  **Install Dependencies:**
    ```bash
    pip install pyspark
    ```
2.  **Dataset Configuration:** Ensure `car_sales.txt` and `assignment.text.20.json` are accessible in your environment (e.g., local directory or mounted Google Drive).
3.  **Execution:** Run the Jupyter Notebook cells to initialize the `SparkSession` and trigger the data processing pipelines.
