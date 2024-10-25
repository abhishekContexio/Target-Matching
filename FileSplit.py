# Description
    # Script written to split large data into equal parts and put this split data into seprate output files Was return
    # for requirement  MTA_value_Hierarchy for splitting the large data sent by team but script was never used because of
    # requirement change

from pymongo import MongoClient
import pandas as pd
import pymongo

# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Target"]
collection = mydb["MTA-Value_Hierarchy_internal"]
part_size = 0
try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    # Get the total count of documents in the collection
    total_records = collection.count_documents({})
    print(f'This is the collection count: {total_records}')

    # Split the collection into three parts
    part_size = total_records // 3  # Split into 3 parts equally
    remaining_records = total_records % 3  # Handle leftover records
except Exception as e:
    print(f"Error: {e}")


# Helper function to write a cursor to a CSV file
def write_to_csv(part_number, cursor):
    df = pd.DataFrame(list(cursor))
    excel_output_path = "C:/Users/Abhishek/Downloads/MTA_value_Hierarchy_Internal.xlsx"
    df.to_excel(excel_output_path, index=False)
    print(f'Created: {excel_output_path}')


# Read the collection and split into 3 parts
for part in range(1, 4):
    skip_count = (part - 1) * part_size
    limit_count = part_size + (1 if part == 3 else 0)  # Add extra records to the last part if needed

    # Retrieve documents for the current part using skip and limit
    cursor = collection.find().skip(skip_count).limit(limit_count)

    # Write to CSV
    write_to_csv(part, cursor)

print("Completed splitting the collection into three parts.")