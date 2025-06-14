# ETL Extract Lab

**Name**: Yar Deng Kuot  
**Student ID**: YourStudentID  

## Description
This project implements Full and Incremental Extraction for a sales dataset as part of DSA 2040A Lab 3 (US 2025 Take-Home Lab). The dataset contains 100 sales records with columns: `order_id`, `order_date`, `product_name`, `price`, `customer_name`, and `last_updated`. The Jupyter Notebook (`etl_extract.ipynb`) demonstrates:
- **Full Extraction**: Loads all records from `custom_data.csv`.
- **Incremental Extraction**: Loads only records updated since the last extraction, tracked using `last_extraction.txt`.

The notebook includes clear documentation for each step, and all required files are included in this repository.

## Tools Used
- **Python**: Programming language for the ETL pipeline.
- **pandas**: Library for data handling and manipulation.
- **Faker**: Library for generating synthetic sales data.
- **Jupyter Notebook**: Environment for coding and documentation.
- **Git**: Version control for the repository.

## How to Reproduce
1. Clone the repository:



Open etl_extract.ipynb and run all cells.
Ensure custom_data.csv and last_extraction.txt are in the same folder as the notebook.

Data Source
The dataset (custom_data.csv) was generated using Python’s Faker library, creating 100 realistic sales records with fields for order ID, order date, product name, price, customer name, and last-updated timestamp.
Screenshots
(To add screenshots, run the notebook, capture outputs (e.g., Full Extraction results) using a tool like Snipping Tool, save them in a screenshots folder, and link them here, e.g., ![Full Extraction](screenshots/full_extraction.png).)```


# ETL EXTRACT Lab5
ETL Pipeline: Extract and Transform
DSA 2040A - Lab 3 & Lab 4
Student Name: Yar Deng Kuot
Student ID: [Your Student ID]

Overview
--------
This repository contains the ETL (Extract, Transform, Load) pipeline developed for Labs 3 and 4 of DSA 2040A. Lab 3 focused on data extraction, while Lab 4 extends the pipeline with transformation logic to clean, enrich, and structure sales data for analysis. The pipeline is implemented as a Python script (etl_extract.py) designed to run in Visual Studio Code (VS Code).

Project Structure
-----------------
ETL_Extract_<YourName>_<StudentID>/
├── etl_extract.py              # Python script for ETL pipeline
├── raw_full.csv                # Raw full dataset (generated)
├── raw_incremental.csv         # Raw incremental dataset (generated)
├── transformed_full.csv        # Transformed full dataset
├── transformed_incremental.csv # Transformed incremental dataset
├── .gitignore                  # Git ignore file
├── README.txt                  # Project documentation

Dataset Description
-------------------
- Full Dataset: Contains 10 sales records with columns:
  - order_id: Unique order identifier (includes one duplicate: 1003).
  - customer_id: Unique customer identifier.
  - order_date: Order date with mixed formats (e.g., 2023-10-01, 10/15/2023) and one missing value.
  - quantity: Number of items ordered, with one missing value and one zero.
  - unit_price: Price per item, with one missing value.
  - product_category: Product category (e.g., Electronics, Clothing), with one missing value.
- Incremental Dataset: Contains 3 new sales records with similar columns, including missing values in quantity and unit_price.

Transformations Applied
----------------------
The transform_data function in etl_extract.py applies the following transformations:
1. Cleaning:
   - Removes duplicates based on order_id.
   - Drops rows with missing values in order_date, quantity, or product_category.
   - Fills missing unit_price values with the column's mean.
2. Enrichment:
   - Adds a total_price column, calculated as quantity * unit_price.
3. Structural:
   - Standardizes order_date to YYYY-MM-DD format.
   - Converts quantity to integer and unit_price to float.

How to Run in VS Code
---------------------
1. Clone the Repository:
   git clone <repository-url>
2. Install Dependencies:
   - Ensure Python (3.7 or higher) is installed and added to your PATH.
   - Install required libraries:
     pip install pandas numpy
3. Open in VS Code:
   - Open the project folder (ETL_Extract_<YourName>_<StudentID>) in VS Code via File > Open Folder.
   - Install the Python extension (by Microsoft) in VS Code.
4. Run the Script:
   - Open etl_extract.py.
   - Click the "Run Python File" button (play icon in the top-right corner), or run in the terminal:
     python etl_extract.py
   - The script generates:
     - raw_full.csv: Raw full dataset.
     - raw_incremental.csv: Raw incremental dataset.
     - transformed_full.csv: Transformed full dataset.
     - transformed_incremental.csv: Transformed incremental dataset.
5. Verify Outputs:
   - Check the console for printed raw and transformed datasets.
   - Open the generated CSV files to confirm transformations.

Outputs
-------
- Console Output: Displays raw and transformed datasets for both full and incremental data.
- CSV Files:
  - transformed_full.csv: Transformed full dataset with cleaned, enriched, and structured data.
  - transformed_incremental.csv: Transformed incremental dataset with the same transformations.

Notes
-----
- Replace [Your Name] and [Your Student ID] in etl_extract.py and README.txt with your details.
- The .gitignore file excludes CSV files and Python cache files to prevent committing large or temporary files.
- Ensure you have write permissions in the project folder to save CSV files.
- The script assumes the datasets are generated synthetically for demonstration. Replace with actual data sources if needed.

GitHub Setup
------------
1. Initialize a Git repository:
   git init
   git add etl_extract.py .gitignore README.txt
   git commit -m "Initial commit for ETL pipeline"
2. Create a GitHub repository named ETL_Extract_<YourName>_<StudentID>.
3. Push to GitHub:
   git remote add origin <your-repository-url>
   git push -u origin main
