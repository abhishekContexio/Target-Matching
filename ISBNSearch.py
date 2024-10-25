# Description
    #Python script to search ISBN value inside Long_Description attribute for each month data of walmart P4
    # and return all data of matched records
import pymongo
import pandas as pd
import openpyxl

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]


months = ["JAN","FEB","MAR","APR","MAY","JUN", "JUL", "AUG", "Sep"]
all_matched_records = []
try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    for month in months:
        collection_name = f"{month}_WalmartData"
        print("Collection name: "+collection_name)
        collection = mydb[collection_name]

        query = {"Long_Description": {"$regex": "ISBN"}}
        matched_records = list(collection.find(query))

        matched_count = len(matched_records)
        print(f'Matched records for collection {collection_name}:{matched_count}')

        all_matched_records.extend(matched_records)
    if all_matched_records:
        df = pd.DataFrame(all_matched_records)
        df.to_excel("Matched_ISBN_Records.xlsx", index=False)
        print(f'Exported {len(all_matched_records)} matched records to "Matched_ISBN_Records"')
    else:
        print("No records matched")
except Exception as e:
    print(f"An error occurred: {e}")