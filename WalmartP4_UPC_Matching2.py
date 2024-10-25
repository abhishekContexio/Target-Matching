# Description
    # Same as Walmart_P4_UPC_Matching only a couter is added here
    # The script now removes any leading apostrophe from the UPC identifier before matching.
import pymongo
import pandas as pd
import openpyxl

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["UPC_Values"]

months = ["JUN", "JUL", "AUG", "Sep"]
projection = {"Identifier": 1, "_id": 0}

try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    collection1_data = collection1.find({}, projection)
    results = []
    count = 0  # Counter for processed products

    for document in collection1_data:
        identifier = document.get("Identifier", "N/A")
        if identifier == "N/A":
            continue

        # Strip the ' symbol if it exists
        identifier = identifier.strip("'")

        count += 1
        if count % 100 == 0:
            print(f"Processed {count} UPCs")

        for month in months:
            collection_name = f"{month}_WalmartData"
            collection = mydb[collection_name]

            # Creating index only if it doesn't exist
            collection.create_index([("GTIN13", pymongo.ASCENDING)], background=True)

            # Handle different GTIN types
            if month == "Sep":
                match = collection.find_one({'GTIN13': identifier})
            else:
                match = collection.find_one({
                    "$or": [
                        {'GTIN13': identifier},
                        {'GTIN13': float(identifier)}  # Check float only for non-Sep months
                    ]
                })

            if match:
                results.append([identifier, month])
                break

    print("Results list:", results)

    # Create DataFrame from results and save to Excel
    df = pd.DataFrame(results, columns=["Identifier", "Matched Month"])
    output_file = "C:\\Users\\Abhishek\\Downloads\\WalmartDataOutput.xlsx"
    df.to_excel(output_file, sheet_name="Results", index=False)
    print("Output written to Excel file:", output_file)

except Exception as e:
    print(f"An error occurred: {e}")
