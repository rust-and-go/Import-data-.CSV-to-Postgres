import os
import glob
import chardet
import psycopg2
import logging
from dotenv import load_dotenv
from urllib.parse import quote_plus

# ------------------- Connect ------------------- #
load_dotenv()  # Load
logging.basicConfig(
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
  level=logging.INFO)  # Check Log


connection = psycopg2.connect(
    host=quote_plus(os.getenv("HOST")),
    port=quote_plus(os.getenv("PORT")),
    dbname=quote_plus(os.getenv("POSTGRES_DB")),
    user=quote_plus(os.getenv("POSTGRES_USER")),
    password=quote_plus(os.getenv("POSTGRES_PASSWORD"))
)
# Create cursor
cursor = connection.cursor()

# Execute query to get list of tables
query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
"""
cursor.execute(query)

# Get the results and print them to the console
tables = cursor.fetchall()
for idx, table in enumerate(tables, start=1):
    print(f"{idx}. {table[0]}")

# Ask the user to select a table by sequence number
table_choice = int(input("Enter the table number of the table you want to select: "))

# Get table name from sequence number
selected_table = tables[table_choice - 1][0]
print(f"You have selected the table: {selected_table}")

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# Get the list of .csv files in the directory
folder_path = 'inputCSV'  # Path to the directory containing the .csv files
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

# Print a list of .csv files to the console with sequential numbers
for idx, file in enumerate(csv_files, start=1):
    print(f"{idx}. {os.path.basename(file)}")

# Ask the user to select a file by sequence number
file_choice = int(input("Enter the serial number of the file you want to import: "))

# Get the file path from the sequence number
selected_file_path = csv_files[file_choice - 1]
print(f"You have selected the file: {os.path.basename(selected_file_path)}")

# Determine the encoding of the selected file
encoding = detect_encoding(selected_file_path)
print(f'The encoding of the file is: {encoding}')

# Define SQL command to import data
sql = f"""
    COPY {selected_table}
    FROM STDIN
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
"""

# Open the data file and execute the COPY command
try:
    with open(selected_file_path, 'r', encoding=encoding) as f:
        cursor.copy_expert(sql, f)
        connection.commit()  # Confirm changes
        print(f"Entered data into the table {selected_table} success.")
except Exception as e:
    connection.rollback()  # Undo changes if there are errors
    print(f"Cannot import data: {e}")

# Close the cursor and connect
cursor.close()
connection.close()



