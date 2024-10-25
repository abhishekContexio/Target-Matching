# Description
    # This python script compares UPC values between two mongoDb collections Target data and Walmart P4 data
    # It checks if UPC values from Target are present in Walmart data
    # For each matched record it stores the result in an excel file
    # The output includes column for UP, matched collection, matched status
import pymongo
import pandas as pd
import openpyxl
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["target_data5"]
collection2 = mydb["AUG_WalmartData"]

unique_id_field = "upc"
try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    cursor = collection1.find({}, {unique_id_field: 1})

    results = []
    index = 0
    for doc in cursor:
        id_value = doc.get(unique_id_field)
        if id_value:  # Check if id_value is not an empty string
            if id_value == "'":
                print("Skipping empty value 1")
                continue
            try:
                match = collection2.find_one({'UPC': id_value})
                if match:
                    print(id_value + " present in both collections: True")
                    results.append([id_value, "Walmart", True])
                else:
                    print(id_value + " not present in both collections: False")
                    results.append([id_value, "", False])
            except ValueError:
                results.append([id_value, "Error", False])

        else:
            print("Skipping empty id_value 2")
            results.append([id_value, "Empty ID", False])
        if index % 100 == 0:
            print(str(index)+ " Products processed")
        index +=1
    df = pd.DataFrame(results, columns=["UniqueId", "Matched Collection", "Status"])
    df.to_excel("C:\\Users\\Abhishek\\Downloads\\output.xlsx", index=False)
    print("Output written")
except Exception as e:
    print(e)


