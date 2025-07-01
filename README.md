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



##ETL Project
Lab 6 – Load

Overview

This section explains that Lab 5 is the final phase of the ETL (Extract, Transform, Load) project, focusing on loading the transformed datasets from Lab 4 (transformed_full.csv and transformed_incremental.csv) into structured storage formats for analysis and querying. It emphasizes that the process is automated using a Python script named etl_load.py, located in the project directory ETL_Extract_<YourName>_<StudentID>/. The overview clarifies that this lab ensures the data is stored efficiently in formats suitable for both small-scale applications and larger data processing pipelines. It highlights the continuation from Lab 4, where the datasets were transformed, and explains that Lab 5 prepares the data for downstream use, such as generating reports, performing analytics, or integrating with other systems.

Project Structure

This section describes the organization of the project directory, which includes:

The transformed_full.csv file, containing the complete transformed dataset from Lab 4.
The transformed_incremental.csv file, containing the incremental transformed dataset from Lab 4.
The etl_extract.ipynb file, a Jupyter Notebook from previous labs (e.g., Lab 4) for data extraction and transformation.
The etl_load.py file, the Python script created for Lab 5 to handle data loading.
The README.md file itself, providing project documentation.
The .gitignore file, which specifies files and directories to exclude from version control.
A loaded_data/ directory, which stores the output files: SQLite databases (full_data.db and incremental_data.db) and Parquet files (full_data.parquet and incremental_data.parquet).
This structure ensures all components are organized and accessible within the project folder.
Prerequisites

This section outlines the requirements for running the etl_load.py script:

A Python environment with version 3.8 or higher is needed.
Required Python packages include pandas for data manipulation and Parquet file handling, and pyarrow for efficient Parquet operations. These can be installed using a package manager like pip.
The sqlite3 module, which is included with Python, is used for database operations, so no additional installation is required.
The input files (transformed_full.csv and transformed_incremental.csv) must be present in the project directory, as they are the outputs from Lab 4.
Visual Studio Code (VS Code) is recommended as the development environment, with the Python extension installed to support script execution and debugging.
Loading Method Used

This section provides an in-depth explanation of the two storage formats used:

SQLite Databases: The script loads each dataset into a separate SQLite database: full_data.db for the full dataset and incremental_data.db for the incremental dataset. SQLite is described as a lightweight, file-based database system that requires no server setup, making it ideal for local storage and SQL-based querying. The schema for each database table includes an id column as the primary key (integer), with other columns like customer name and product as text, quantity as an integer, unit price and total price as real numbers, and order date as text. This schema ensures data integrity and supports efficient querying.
Parquet Files: The script also saves each dataset as a Parquet file: full_data.parquet and incremental_data.parquet. Parquet is described as a columnar storage format optimized for big data applications, offering high compression and fast read/write performance. It’s compatible with tools like pandas, Apache Spark, and other data processing frameworks, making it suitable for scalable analytics. The benefits of both formats are highlighted, noting SQLite’s simplicity for local use and Parquet’s efficiency for large datasets.
Sample Code

This section mentions that the etl_load.py script includes a section for loading the full dataset into a SQLite database. It describes the process of connecting to the database, writing the data to a table with a predefined schema, and closing the connection, without displaying the actual code. It notes that similar logic is applied to the incremental dataset and for saving both datasets as Parquet files, ensuring consistency across all loading operations.

Output Location

This section specifies where the loaded data is stored:

SQLite Databases: The full dataset is stored in loaded_data/full_data.db as a table named full_data, and the incremental dataset is stored in loaded_data/incremental_data.db as a table named incremental_data. These databases can be queried using SQL for analysis.
Parquet Files: The full dataset is saved as loaded_data/full_data.parquet, and the incremental dataset as loaded_data/incremental_data.parquet. These files are optimized for storage and can be read by data processing tools for further analysis.
The section explains that the loaded_data/ directory is created automatically by the script if it doesn’t exist.
Usage Instructions

This section provides a step-by-step guide to running the script:

Prepare the Environment: Install Python 3.8+, the required packages (pandas and pyarrow), and ensure the input CSV files from Lab 4 are in the project directory.
Run the Script: Open VS Code, navigate to the project directory, and execute etl_load.py using a terminal command (python etl_load.py). The script will check for input files, create the output directory, load the data into SQLite and Parquet formats, and print confirmation messages.
Verify Output: The script includes a verification step that displays the first five rows of each SQLite table and Parquet file to confirm successful loading.
GitHub Update: Commit the etl_load.py, README.md, and .gitignore files to your GitHub repository. The .gitignore file ensures that output files (databases and Parquet files) are not included in version control. Use standard Git commands to add, commit, and push changes to the repository.
Verification

This section explains that the script includes a verification process to ensure the data was loaded correctly. It queries the SQLite databases to display the first five rows of the full_data and incremental_data tables and reads the Parquet files to show their first five rows using pandas. This step confirms that the data matches the expected schema and is accessible for further use.

Notes

This section includes additional information:

The project directory name should be updated with your actual name and student ID (replacing <YourName>_<StudentID>).
The script assumes the input CSV files have the expected columns (id, customer_name, product, quantity, unit_price, total_price, order_date) from Lab 4.
If the input files are missing, the script will raise an error with a clear message indicating the issue.
The SQLite schema is explicitly defined to maintain data consistency.
The optional PostgreSQL/MySQL loading target was not implemented, as it wasn’t required for this lab.
Users can extend the script by modularizing code into an etl_utils.py file if desired, though this is not included in the current implementation.
.gitignore Description
The .gitignore file is configured to exclude specific files and directories from version control to keep the GitHub repository clean. It prevents the loaded_data/ directory, all SQLite database files (with .db extension), and all Parquet files (with .parquet extension) from being committed. This ensures that only source files like etl_load.py, README.md, and .gitignore are tracked, while generated output files are ignored.
