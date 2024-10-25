# Description
    # This Python script compares UniqueId from the Target_Success_Data2 MongoDB collection with the UPC from Walmart data collections, based on the month field from Target data.
    # It dynamically selects the Walmart collection by using the month value (e.g., for August, the collection is named AUG_WalmartData) and retrieves details such as Brand, Product_Name, UPC, and other attributes.
    # The matching records are written to an Excel file, WalmartDataOutput.xlsx, including all relevant data fields.

import pymongo
import pandas as pd
import openpyxl

def get_collection_name(month):
    return f"{month[:3].upper()}_WalmartData"

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["Target_Success_Data2"]

try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    projection = {"UniqueId": 1, "Status": 1, "Month": 1, "_id": 0}
    collection1_data = collection1.find({}, projection)

    results = []

    for document in collection1_data:
        unique_id = document.get("UniqueId", "N/A")
        print("unique_id: "+unique_id)
        status = document.get("Status", "N/A")
        print("status: "+str(status))
        month = document.get("Month", "N/A")
        print("month: "+month)

        if not unique_id or not month:
            continue;

        collection2_name = get_collection_name(month)
        print("collection2_name: "+collection2_name)
        collection2 = mydb[collection2_name]
        collection2.create_index([("UPC", pymongo.ASCENDING)])
        collection2_projection = {
            "All_Image_URL": 1,
            "Brand": 1,
            "Breadcrumb": 1,
            "Category": 1,
            "Condition": 1,
            "Dimensions": 1,
            "EAN": 1,
            "GTIN13": 1,
            "Item_ID": 1,
            "Main_Image_URL": 1,
            "Model_ID": 1,
            "Product_ID": 1,
            "Product_Name": 1,
            "Product_URL": 1,
            "Selected color": 1,
            "Selected size": 1,
            "UPC": 1,
            "_id": 0
        }

        match = collection2.find_one({'UPC': unique_id}, collection2_projection)
        if match:
            print("Match present")
            results.append([
                unique_id,
                "Walmart",
                status,
                month,
                match.get("All_Image_URL", "N/A"),
                match.get("Brand", "N/A"),
                match.get("Breadcrumb", "N/A"),
                match.get("Category", "N/A"),
                match.get("Condition", "N/A"),
                match.get("Dimensions", "N/A"),
                match.get("EAN", "N/A"),
                match.get("GTIN13", "N/A"),
                match.get("Item_ID", "N/A"),
                match.get("Main_Image_URL", "N/A"),
                match.get("Model_ID", "N/A"),
                match.get("Product_ID", "N/A"),
                match.get("Product_Name", "N/A"),
                match.get("Product_URL", "N/A"),
                match.get("Selected color", "N/A"),
                match.get("Selected size", "N/A")
            ])
        else:
            print("UPC not found in both the collections")
    print("Results: "+str(results))
    columns = [
            "UPC", "MatchedCollection", "Status", "Month",
            "All_Image_URL", "Brand", "Breadcrumb", "Category", "Condition",
            "Dimensions", "EAN", "GTIN13", "Item_ID", "Main_Image_URL",
            "Model_ID", "Product_ID", "Product_Name", "Product_URL",
            "Selected color", "Selected size"
    ]
    df = pd.DataFrame(results,columns=columns)
    output_file = "C:\\Users\\Abhishek\\Downloads\\WalmartDataOutput.xlsx"
    df.to_excel(output_file,sheet_name="Results", index=False)
    print("Output written")
except Exception as e:
        print(f"An error occurred: {e}")