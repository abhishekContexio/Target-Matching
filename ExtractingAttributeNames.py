# Description
    # Script to read all data from a collection and write to an output file
from pymongo import MongoClient
import pandas as pd

# Step 1: Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Update with your connection details
db = client['Target']  # Replace with your database name
collection = db['MTA-Value_Hierarchy_internal']  # Replace with your collection name

# Step 2: Retrieve all documents from the collection
documents = list(collection.find().limit(50000))

# Step 3: Convert the documents into a DataFrame
# If the collection is empty, handle that case
if documents:
    df = pd.json_normalize(documents)  # Flatten the documents to a DataFrame
else:
    df = pd.DataFrame()  # Create an empty DataFrame if no documents are found

# Step 4: Write the DataFrame to an Excel file
output_path = "C:/Users/Abhishek/Downloads/MTA-Value_Hierarchy_internal_50k_Records.xlsx"  # Define the path where you want the file saved
df.to_excel(output_path, index=False)

print(f"Data has been exported to {output_path}")
