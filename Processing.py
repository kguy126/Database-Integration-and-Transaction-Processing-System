import mysql.connector
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection(db_config):
    """Establish and return a database connection and cursor."""
    try:
        con = mysql.connector.connect(**db_config)
        cursor = con.cursor()
        logging.info("Database connection successful.")
        return con, cursor
    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")
        return None, None


def fetch_existing_ids(cursor, table_name, id_column, name_column, email_column=None, phone_column=None):
    """Fetch existing IDs and names from a table and return them as a mapping dictionary."""
    if table_name == "Clients":
        cursor.execute(f"SELECT {id_column}, {name_column}, {email_column}, {phone_column} FROM {table_name}")
        return {(name, email, phone): id for id, name, email, phone in cursor.fetchall()}
    else:
        cursor.execute(f"SELECT {id_column}, {name_column} FROM {table_name}")
        return {name: id for id, name in cursor.fetchall()}


def insert_stores(cursor, df, store_map):
    """Insert stores into the Stores table."""
    new_stores = set(df["store_name"].unique()) - set(store_map.keys())
    if new_stores:
        cursor.executemany(
            "INSERT INTO Stores (store_name) VALUES (%s)",
            [(s,) for s in new_stores]
        )
        logging.info(f"Inserted {len(new_stores)} new stores.")


def insert_clients(cursor, df, client_map):
    """Insert clients into the Clients table, using email and phone number to differentiate."""
    # Extract unique clients from the DataFrame
    clients = df[["client_name", "email_address", "phone_number"]].drop_duplicates()
    
    # Filter out clients that already exist in the database
    new_clients = []
    for _, row in clients.iterrows():
        client_key = (row["client_name"], row["email_address"], row["phone_number"])
        if client_key not in client_map:
            new_clients.append((row["client_name"], row["email_address"], row["phone_number"]))
            client_map[client_key] = None  # Mark as inserted

    # Insert new clients
    if new_clients:
        cursor.executemany(
            "INSERT INTO Clients (client_name, email_address, phone_number) VALUES (%s, %s, %s)",
            new_clients
        )
        logging.info(f"Inserted {len(new_clients)} new clients.")


def insert_sales_representatives(cursor, df, sales_rep_map):
    """Insert sales representatives into the SalesRepresentatives table."""
    new_sales_reps = set(df["sales_representative_name"].unique()) - set(sales_rep_map.keys())
    if new_sales_reps:
        cursor.executemany(
            "INSERT INTO SalesRepresentatives (sales_representative_name) VALUES (%s) ON DUPLICATE KEY UPDATE sales_representative_name=sales_representative_name",
            [(sr,) for sr in new_sales_reps]
        )
        logging.info(f"Inserted {len(new_sales_reps)} new sales representatives.")


def insert_products(cursor, df, product_map, store_map):
    """Insert products into the Products table."""
    new_products = df[["product_name", "store_name", "price"]].drop_duplicates()
    new_products["store_id"] = new_products["store_name"].map(store_map)
    new_products = new_products[~new_products["product_name"].isin(product_map.keys())]
    if not new_products.empty:
        cursor.executemany(
            "INSERT INTO Products (product_name, store_id, price) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE price=VALUES(price)",
            [(row["product_name"], row["store_id"], row["price"]) for _, row in new_products.iterrows()]
        )
        logging.info(f"Inserted {len(new_products)} new products.")


def insert_transactions(cursor, df, store_map, client_map, sales_rep_map):
    """Insert transactions into the Transactions table."""
    transactions = df[["transaction_id", "transaction_date", "store_name", "sales_representative_name", "client_name", "email_address", "phone_number"]].drop_duplicates()
    transactions["store_id"] = transactions["store_name"].map(store_map)
    transactions["client_id"] = transactions.apply(
        lambda row: client_map.get((row["client_name"], row["email_address"], row["phone_number"])), axis=1
    )
    transactions["sales_rep_id"] = transactions["sales_representative_name"].map(sales_rep_map)
    transactions = transactions[["transaction_id", "transaction_date", "store_id", "client_id", "sales_rep_id"]]
    cursor.executemany(
        "INSERT INTO Transactions (transaction_id, transaction_date, store_id, client_id, sales_rep_id) VALUES (%s, %s, %s, %s, %s)",
        [tuple(row) for row in transactions.itertuples(index=False, name=None)]
    )
    logging.info(f"Inserted {len(transactions)} new transactions.")


def insert_transaction_products(cursor, df, product_map, store_map):
    """Link transactions to products in the Transaction_Products table."""
    transaction_products = df[["transaction_id", "product_name", "store_name", "quantity"]].drop_duplicates()
    transaction_products["store_id"] = transaction_products["store_name"].map(store_map)
    transaction_products["product_id"] = transaction_products.apply(
        lambda row: product_map.get((row["product_name"], row["store_id"])), axis=1
    )
    transaction_products = transaction_products[["transaction_id", "product_id", "quantity"]]
    cursor.executemany(
        "INSERT INTO Transaction_Products (transaction_id, product_id, quantity) VALUES (%s, %s, %s)",
        [tuple(row) for row in transaction_products.itertuples(index=False, name=None)]
    )
    logging.info(f"Inserted {len(transaction_products)} new transaction-product relationships.")


def process_csv_file(csv_file, db_config):
    """Process a CSV file and insert its data into the database."""
    con, cursor = get_db_connection(db_config)
    if not con:
        return

    try:
        df = pd.read_csv(csv_file)
        
        # Fetch existing IDs
        store_map = fetch_existing_ids(cursor, "Stores", "store_id", "store_name")
        client_map = fetch_existing_ids(cursor, "Clients", "client_id", "client_name", "email_address", "phone_number")
        sales_rep_map = fetch_existing_ids(cursor, "SalesRepresentatives", "sales_rep_id", "sales_representative_name")
        product_map = fetch_existing_ids(cursor, "Products", "product_id", "product_name")

        # Insert reference data
        insert_stores(cursor, df, store_map)
        insert_clients(cursor, df, client_map)
        insert_sales_representatives(cursor, df, sales_rep_map)
        insert_products(cursor, df, product_map, store_map)
        con.commit()

        # Fetch updated IDs (in case new records were inserted)
        store_map = fetch_existing_ids(cursor, "Stores", "store_id", "store_name")
        client_map = fetch_existing_ids(cursor, "Clients", "client_id", "client_name", "email_address", "phone_number")
        sales_rep_map = fetch_existing_ids(cursor, "SalesRepresentatives", "sales_rep_id", "sales_representative_name")
        product_map = fetch_existing_ids(cursor, "Products", "product_id", "product_name")

        # Insert transactions and transaction-products
        insert_transactions(cursor, df, store_map, client_map, sales_rep_map)
        insert_transaction_products(cursor, df, product_map, store_map)
        con.commit()

        logging.info(f"Data from {csv_file} uploaded successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error processing {csv_file}: {err}")
        con.rollback()
    finally:
        cursor.close()
        con.close()


if __name__ == "__main__":
    db_config = {
        "host": "",
        "user": "",
        "password": "",
        "database": ""
    }

    # List of CSV files to process
    csv_files = [
        r"C:\Users\deji_\Downloads\transactions.csv",
        r"C:\Users\deji_\Downloads\transactions_new.csv"
    ]

    for csv_file in csv_files:
        process_csv_file(csv_file, db_config)