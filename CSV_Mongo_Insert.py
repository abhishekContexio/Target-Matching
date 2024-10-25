# Description
    # Python script to import CSV files into Mongo
import os
import pandas as pd
from pymongo import MongoClient


MONGO_URI = 'mongodb://localhost:27017/'  # Change this to your MongoDB connection URI
DB_NAME = 'Target'  # Replace with your database name
COLLECTION_NAME = 'Text_Specs_Batch1_Part1'  # Replace with your collection name

# Directory containing the CSV files
CSV_DIRECTORY = r"D:/PlatformX files/Target/Converted_Excel_Files/Part1_CSV"


# Function to import CSV data into MongoDB
def import_csv_to_mongodb(csv_directory, mongo_uri, db_name, collection_name):
    try:
        # Initialize MongoDB client
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Loop through each file in the directory
        for filename in os.listdir(csv_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(csv_directory, filename)

                # Read the CSV file into a DataFrame with error handling for encoding
                try:
                    df = pd.read_csv(file_path, keep_default_na=False, dtype=str, encoding='utf-8')
                except UnicodeDecodeError:
                    # If there's a decoding error, use a fallback encoding (ISO-8859-1)
                    df = pd.read_csv(file_path, keep_default_na=False, dtype=str, encoding='ISO-8859-1')

                # Convert DataFrame to dictionary and insert into MongoDB
                data = df.to_dict(orient='records')
                collection.insert_many(data)

                print(f'Successfully imported {filename} into MongoDB.')

    except Exception as e:
        print(f'Error: {e}')
    finally:
        client.close()


# Call the function to import CSV files
import_csv_to_mongodb(CSV_DIRECTORY, MONGO_URI, DB_NAME, COLLECTION_NAME)
