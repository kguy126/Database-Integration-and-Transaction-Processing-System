Create Database project;
Use project;

CREATE TABLE Stores (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE SalesRepresentatives (
    sales_rep_id INT AUTO_INCREMENT PRIMARY KEY,
    sales_representative_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Clients (
    client_id INT AUTO_INCREMENT PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255),
    phone_number VARCHAR(20),
    UNIQUE (client_name, email_address, phone_number)
);

CREATE TABLE Products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    store_id INT NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    UNIQUE (product_name, store_id),
    FOREIGN KEY (store_id) REFERENCES Stores(store_id) ON DELETE CASCADE
);

CREATE TABLE Transactions (
    transaction_id VARCHAR(45) PRIMARY KEY NOT NULL,
    transaction_date DATE NOT NULL,
    store_id INT NOT NULL,
    client_id INT NOT NULL,
    sales_rep_id INT NOT NULL,
    FOREIGN KEY (store_id) REFERENCES Stores(store_id) ON DELETE CASCADE,
    FOREIGN KEY (client_id) REFERENCES Clients(client_id) ON DELETE CASCADE,
    FOREIGN KEY (sales_rep_id) REFERENCES SalesRepresentatives(sales_rep_id) ON DELETE CASCADE
);

CREATE TABLE Transaction_Products (
    transaction_id VARCHAR(45),
    product_id INT,
    quantity INT NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (transaction_id, product_id),
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES Products(product_id) ON DELETE CASCADE
);