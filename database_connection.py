import mysql.connector
import pandas as pd
import os
import time
import io
import re
import logging
import threading

# Configure logging
logging.basicConfig(filename='edgar_parser.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# Database connection parameters
db_config = {
    'user': 'tony',
    'password': 'tony123',
    'host': 'localhost',
    'database': 'tony'
}

# Lock for thread-safe file operations (if needed)
file_lock = threading.Lock()

# Connect to the MySQL database
try:
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor()
    logging.info("Successfully connected to the database.")
except mysql.connector.Error as err:
    logging.error(f"Error connecting to the database: {err}")
    exit(1)

# Function to create tables and add unique constraint
def create_tables():
    # SQL statements to create tables
    create_companies_table = """
    CREATE TABLE IF NOT EXISTS companies (
        cik VARCHAR(10) PRIMARY KEY,
        company_name VARCHAR(255) NOT NULL
    );
    """

    create_filings_table = """
    CREATE TABLE IF NOT EXISTS filings (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cik VARCHAR(10) NOT NULL,
        form_type VARCHAR(20) NOT NULL,
        date_filed DATE NOT NULL,
        filename VARCHAR(255) NOT NULL,
        url VARCHAR(255) NOT NULL,
        FOREIGN KEY (cik) REFERENCES companies(cik),
        UNIQUE KEY unique_filing (cik, form_type, date_filed, filename)
    );
    """

    try:
        cursor.execute(create_companies_table)
        cursor.execute(create_filings_table)
        cnx.commit()
        logging.info("Tables 'companies' and 'filings' created or verified successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error creating tables: {err}")
        cnx.rollback()
        exit(1)

# Function to insert company into the 'companies' table
def insert_company(cik, company_name):
    try:
        cursor.execute("""
            INSERT INTO companies (cik, company_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE company_name = VALUES(company_name);
        """, (cik, company_name))
        cnx.commit()
    except mysql.connector.Error as err:
        logging.error(f"Error inserting company {company_name}: {err}")
        cnx.rollback()

# Function to insert filing into the 'filings' table
def insert_filing(cik, form_type, date_filed, filename, url):
    try:
        cursor.execute("""
            INSERT INTO filings (cik, form_type, date_filed, filename, url)
            VALUES (%s, %s, %s, %s, %s);
        """, (cik, form_type, date_filed, filename, url))
        cnx.commit()
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
            logging.info(f"Duplicate filing detected and skipped: {filename}")
            # Optionally, update the existing record if necessary
        else:
            logging.error(f"Error inserting filing {filename}: {err}")
            cnx.rollback()

# Function to process a single index file
def process_index_file(idx_file_path):
    logging.info(f'Processing index file: {idx_file_path}')
    try:
        with open(idx_file_path, 'r', encoding='latin1') as f:
            content = f.read()

        lines = content.splitlines()
        
        # Find the index of the separator line (line with at least 100 hyphens)
        for i, line in enumerate(lines):
            if len(line.strip()) >= 100 and set(line.strip()) == {'-'}:
                data_start_index = i + 1  # Data starts after this line
                break
        else:
            logging.error(f'Separator line not found in {idx_file_path}')
            return

        # Extract data lines
        data_lines = lines[data_start_index:]

        # Join the data lines back into a single string
        data = '\n'.join(data_lines)

        # Define column specifications based on actual positions
        colspecs = [(0, 62), (62, 74), (74, 86), (86, 98), (98, None)]
        names = ['Company Name', 'Form Type', 'CIK', 'Date Filed', 'Filename']

        # Read the data using read_fwf
        df = pd.read_fwf(io.StringIO(data), colspecs=colspecs, names=names)

        # Filter for relevant form types
        relevant_forms = ['10-K', '10-Q', '8-K']
        df_filtered = df[df['Form Type'].isin(relevant_forms)]

        # Remove rows with missing data
        df_filtered.dropna(subset=['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename'], inplace=True)

        for idx, row in df_filtered.iterrows():
            cik = str(row['CIK']).strip().zfill(10)
            company_name = str(row['Company Name']).strip()
            form_type = str(row['Form Type']).strip()
            date_filed = str(row['Date Filed']).strip()
            filename = str(row['Filename']).strip()
            url = f"https://www.sec.gov/Archives/{filename}"

            # Insert company and filing into database
            insert_company(cik, company_name)
            insert_filing(cik, form_type, date_filed, filename, url)

        logging.info(f'Successfully processed {idx_file_path}')
    except Exception as e:
        logging.error(f'Error processing {idx_file_path}: {e}')

# Function to load processed files
def load_processed_files():
    processed_files = set()
    if os.path.exists('processed_files.txt'):
        with open('processed_files.txt', 'r') as f:
            for line in f:
                processed_files.add(line.strip())
    return processed_files

# Function to save a processed file
def save_processed_file(file_name):
    with file_lock:
        with open('processed_files.txt', 'a') as f:
            f.write(f"{file_name}\n")

# Function to process all index files in a directory, skipping processed files
def process_all_index_files(index_dir):
    processed_files = load_processed_files()
    for root, dirs, files in os.walk(index_dir):
        for file in files:
            if file.endswith('.idx') and file not in processed_files:
                idx_file_path = os.path.join(root, file)
                process_index_file(idx_file_path)
                # Record that this file has been processed
                save_processed_file(file)
                # Sleep to avoid overwhelming resources (adjust as needed)
                time.sleep(0.1)
            else:
                logging.info(f'Skipping already processed file: {file}')

# Main execution
if __name__ == "__main__":
    # Create tables and add unique constraint
    create_tables()

    # Directory containing your index files
    index_directory = 'index_files'  # Update with your actual directory

    # Process all index files
    process_all_index_files(index_directory)

    # Close the database connection
    cursor.close()
    cnx.close()
    logging.info("Database connection closed.")
