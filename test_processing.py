import pytest
import mysql.connector
import pandas as pd
from unittest.mock import MagicMock, patch
import logging
import Processing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# Sample CSV data (mocked)
mock_csv_data = {
    "transaction_id": ["1id", "2id"],
    "transaction_date": ["2024-01-01", "2024-01-02"],
    "store_name": ["Snakes and Lattes", "Second cup"],
    "sales_representative_name": ["John", "Simon"],
    "client_name": ["Joe", "House"],
    "email_address": ["joe@example.com", "house@example.com"],
    "phone_number": ["123-456-7890", "987-654-3210"],
    "product_name": ["London Fog", "Moca"],
    "price": [10.0, 20.0],
    "quantity": [1, 2],
}

# Mocked DataFrame
mock_df = pd.DataFrame(mock_csv_data)

@pytest.fixture
def mock_db():
    """Creates a mocked MySQL connection and cursor."""
    mock_con = MagicMock()
    mock_cursor = MagicMock()
    mock_con.cursor.return_value = mock_cursor
    return mock_con, mock_cursor

def test_database_connection_success(mock_db):
    """Test if database connection is established successfully."""
    mock_con = mock_db[0]
    with patch("mysql.connector.connect", return_value=mock_con):
        conn, cursor = Processing.get_db_connection({})
        assert conn == mock_con
        logging.info("Database connection test passed.")

def test_insert_stores(mock_db):
    """Test if store data is inserted correctly."""
    mock_con, mock_cursor = mock_db
    store_map = {}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_stores(mock_cursor, mock_df, store_map)
        assert mock_cursor.executemany.called
        logging.info("Store insertion test passed.")

def test_insert_clients(mock_db):
    """Test if client data is inserted correctly."""
    mock_con, mock_cursor = mock_db
    client_map = {}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_clients(mock_cursor, mock_df, client_map)
        assert mock_cursor.executemany.called
        logging.info("Client insertion test passed.")

def test_insert_sales_representatives(mock_db):
    """Test if sales representatives are inserted correctly."""
    mock_con, mock_cursor = mock_db
    sales_rep_map = {}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_sales_representatives(mock_cursor, mock_df, sales_rep_map)
        assert mock_cursor.executemany.called
        logging.info("Sales representative insertion test passed.")

def test_insert_products(mock_db):
    """Test if products are inserted correctly."""
    mock_con, mock_cursor = mock_db
    product_map = {}
    store_map = {"Snakes and Lattes": 1, "Second cup": 2}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_products(mock_cursor, mock_df, product_map, store_map)
        assert mock_cursor.executemany.called
        logging.info("Product insertion test passed.")

def test_insert_transactions(mock_db):
    """Test if transactions are inserted correctly."""
    mock_con, mock_cursor = mock_db
    store_map = {"Snakes and Lattes": 1, "Second cup": 2}
    client_map = {("Joe", "joe@example.com", "123-456-7890"): 1, ("House", "house@example.com", "987-654-3210"): 2}
    sales_rep_map = {"John": 1, "Simon": 2}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_transactions(mock_cursor, mock_df, store_map, client_map, sales_rep_map)
        assert mock_cursor.executemany.called
        logging.info("Transaction insertion test passed.")

def test_insert_transaction_products(mock_db):
    """Test if transaction-product relationships are inserted correctly."""
    mock_con, mock_cursor = mock_db
    product_map = {("London Fog", 1): 1, ("Moca", 2): 2}
    store_map = {"Snakes and Lattes": 1, "Second cup": 2}
    with patch("mysql.connector.connect", return_value=mock_con):
        Processing.insert_transaction_products(mock_cursor, mock_df, product_map, store_map)
        assert mock_cursor.executemany.called
        logging.info("Transaction-Product relationship insertion test passed.")

def test_rollback_on_failure(mock_db):
    """Test if rollback is called when an error occurs."""
    mock_con, mock_cursor = mock_db
    mock_cursor.executemany.side_effect = mysql.connector.Error("Insert Error")
    with patch("mysql.connector.connect", return_value=mock_con):
        with pytest.raises(mysql.connector.Error):
            Processing.insert_stores(mock_cursor, mock_df, {})
            mock_con.rollback.assert_called_once()
        logging.info("Rollback test passed.")
