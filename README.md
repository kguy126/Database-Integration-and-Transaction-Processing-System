Database Integration and Transaction Processing System
This Python application processes and integrates sales transaction data from CSV files into a MySQL database. It handles stores, clients, sales representatives, products, and transactions, ensuring data integrity and avoiding duplicates. The system supports incremental updates, allowing users to upload multiple CSV files to update the database with new transactions and related data.

Key Features
Data Integration: Processes CSV files to extract and insert data into a relational MySQL database.

Duplicate Prevention: Uses unique constraints and composite keys to avoid duplicate entries for clients, products, and sales representatives.

Incremental Updates: Supports uploading multiple CSV files to incrementally update the database without duplicating records.

Error Handling and Logging: Implements robust error handling and logging to track data processing and insertion.

Technologies Used
Python for data processing and database interaction.

MySQL for storing and managing relational data.

Pandas for CSV file processing and data manipulation.

Purpose
This system provides a scalable and efficient solution for managing sales transaction data, ensuring data accuracy and consistency while supporting incremental updates.
