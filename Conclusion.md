## Conclusion
This notebook successfully implemented the Extract phase of an ETL pipeline for a sales dataset with 100 records:
- **Full Extraction**: Loaded all records from `custom_data.csv`, displaying the first 5 rows to confirm the data was read correctly.
- **Incremental Extraction**: Loaded only records with a `last_updated` timestamp newer than the last extraction time, stored in `last_extraction.txt`. This ensures efficient processing of new or updated data.
- **Key Takeaways**: Full Extraction is ideal for initial data loads, while Incremental Extraction optimizes updates by processing only changes. The `last_updated` column and `last_extraction.txt` enable tracking of updates.

The code handles cases like missing or empty `last_extraction.txt` files and is documented for clarity, demonstrating a practical ETL extraction process.