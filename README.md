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
