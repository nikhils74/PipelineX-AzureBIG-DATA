#!pip install mysql-connector-python

import mysql.connector
from mysql.connector import Error

hostname = "6bnvj.h.filess.io"
database = "olistproject_slopelack"
port = "3307"
username = "olistproject_slopelack"
password = "697e5b0bf570cd87d28f4f9e093edba086909afe"

try:
    connection = mysql.connector.connect(host=hostname, database=database, user=username, password=password, port=port)
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")



import pandas as pd

order_payments = pd.read_csv("olist_order_payments_dataset.csv")
order_payments.head()
order_payments.shape



import pandas as pd
import mysql.connector
from mysql.connector import Error

# Connection details
hostname = "6bnvj.h.filess.io"
database = "olistproject_slopelack"
port = "3307"
username = "olistproject_slopelack"
password = "697e5b0bf570cd87d28f4f9e093edba086909afe"

# CSV file path
csv_file_path = "olist_order_payments_dataset.csv"

# Table name where the data will be uploaded
table_name = "olist_order_payments"

try:
    # Step 1: Establish a connection to MySQL server
    connection = mysql.connector.connect(
        host=hostname,
        database=database,
        user=username,
        password=password,
        port=port
    )
    if connection.is_connected():
        print("Connected to MySQL Server successfully!")

        # Step 2: Create a cursor to execute SQL queries
        cursor = connection.cursor()

        # Step 3: Drop table if it already exists (for clean insertion)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Table `{table_name}` dropped if it existed.")

        # Step 4: Create a table structure to match CSV file
        create_table_query = f"""
        CREATE TABLE {table_name} (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(20),
            payment_installments INT,
            payment_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created successfully!")

        # Step 5: Load the CSV data into pandas DataFrame
        data = pd.read_csv(csv_file_path)
        print("CSV data loaded into pandas DataFrame.")

        # Step 6: Insert data in batches of 500 records
        batch_size = 500  # Define the batch size
        total_records = len(data)  # Get total records in the DataFrame

        print(f"Starting data insertion into `{table_name}` in batches of {batch_size} records.")
        for start in range(0, total_records, batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]  # Get the current batch of records

            # Convert batch to list of tuples for MySQL insertion
            batch_records = [
                tuple(row) for row in batch.itertuples(index=False, name=None)
            ]

            # Prepare the INSERT query
            insert_query = f"""
            INSERT INTO {table_name}
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s);
            """

            # Execute the insertion query for the batch
            cursor.executemany(insert_query, batch_records)
            connection.commit()  # Commit after each batch
            print(f"Inserted records {start + 1} to {min(end, total_records)} successfully.")

        print(f"All {total_records} records inserted successfully into `{table_name}`.")

except Error as e:
    # Step 7: Handle any errors
    print("Error while connecting to MySQL or inserting data:", e)

finally:
    # Step 8: Close the cursor and connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")