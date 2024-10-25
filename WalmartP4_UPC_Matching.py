# Description
    # Script look for UPC values from a collection named UPC_Values(UPC values given by Kapil) and checks if those values exist in the Walmart data for the months of June, July, August, and September.
    # If a UPC is found, it saves the identifier and the matching month into an Excel file, allowing easy tracking of which identifiers are present in which month's data.
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

    collection1_data = collection1.find({},projection)
    results = []
    for document in collection1_data:
        identifier = document.get("Identifier", "N/A")
        if identifier == "N/A":
            continue
        print(f"Processing identifier: "+identifier)


        for month in months:
            collection_name = f"{month}_WalmartData"
            print(f"Checking in collection: {collection_name}")
            collection = mydb[collection_name]

            collection.create_index([("GTIN13", pymongo.ASCENDING)])

            # Check if UPC exists in this month's collection
            match = collection.find_one({'GTIN13': {"$in": [identifier, float(identifier)]}})
            if match:
                print("Value "+str(identifier)+" found in "+str(month))
                results.append([identifier, month])
                break

    print("Results list: "+str(results))

    # Create DataFrame from results and save to Excel
    df = pd.DataFrame(results, columns=["Identifier", "Matched Month"])
    output_file = "C:\\Users\\Abhishek\\Downloads\\WalmartDataOutput.xlsx"
    df.to_excel(output_file, sheet_name="Results", index=False)
    print("Output written to Excel file:", output_file)

except Exception as e:
        print(f"An error occurred: {e}")