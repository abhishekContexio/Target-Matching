# Description
    # Script compares ProductID's from to collections
    # True_UPC_Data(Matched UPC's data) Walmart_Matchng_Data3(Walmart dump data Jan-Jul)
    # It checks for matching walmart_Product_ID in the second collection and retrieves several associated properties like Item_Id, Retailer_Id, Match_Type, etc
    # Matching records are saved in an excel
    #This script was used to give Gaurav output of 22k records
import pymongo
import pandas as pd
import openpyxl

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["True_UPC_Walmart_Data"]
collection2 = mydb["Walmart_Matching_Data3"]
collection2.create_index([("properties.Item_Id", pymongo.ASCENDING)])

projection = {"Product_ID": 1, "_id": 0}
matching_records = []

counter = 0
batch_size = 500

try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    collection_data = collection1.find({}, projection)
    product_ids = [doc["Product_ID"] for doc in collection_data if "Product_ID" in doc]
    print("These are the item_ids: " + str(product_ids))
    print(f'Total number of items: {len(product_ids)}')
    matching_data = collection2.find(
        {"walmart_Product_ID": {"$in": product_ids}},
        {
            "walmart_Product_ID": 1,
            "properties.Item_Id": 1,
            "properties.Retailer_Id": 1,
            "properties.Match_Type": 1,
            "properties.Match_Type_Comments": 1,
            "properties.Notes": 1,
            "properties.URL": 1,  # Amazon URL
            "properties.Walmart_URL": 1,
            "properties.asin": 1,
            "_id": 0
        }
    )

    for record in matching_data:
        counter += 1
        if counter % batch_size == 0:
            print(f'Processed {counter} matching records')

        matching_records.append({
            "ProductID": record.get("walmart_Product_ID", ""),
            "ItemID": record["properties"].get("Item_Id", ""),
            "RetailorID": record["properties"].get("Retailer_Id", ""),
            "MatchType": record["properties"].get("Match_Type", ""),
            "MatchTypeComments": record["properties"].get("Match_Type_Comments", ""),
            "Notes": record["properties"].get("Notes", ""),
            "AmazonURL": record["properties"].get("URL", ""),  # Amazon URL
            "WalmartURL": record["properties"].get("Walmart_URL", ""),
            "ASIN": record["properties"].get("asin", "")
        })
    print("Matching records list: "+str(matching_records))
    if matching_records:
        df = pd.DataFrame(matching_records,
                          columns=["ProductID", "ItemID", "RetailorID", "MatchType", "MatchTypeComments", "Notes", "AmazonURL",
                                   "WalmartURL", "ASIN"])
        excel_filename = "C:\\Users\\Abhishek\\Downloads\\matching_records.xlsx"
        df.to_excel(excel_filename, index=False)
        print("Matching records saved to " + excel_filename)
    else:
        print("No matching records to save")

except Exception as e:
    print(f"An error occurred: {e}")