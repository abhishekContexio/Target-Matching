# Description
    # Same as TargetDataCompare with the aditon of Month column
import pymongo
import pandas as pd
import openpyxl

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["target_OneLakhData11"]
collection2 = mydb["May_WalmartData"]
collection2.create_index([("UPC", pymongo.ASCENDING)])  # Updated index to UPC

unique_id_field = "upc"
month_value = "May"
try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    collection1_ids = collection1.distinct(unique_id_field)
    collection1_ids_len = len(collection1_ids)
    print("Collection id's length: " + str(collection1_ids_len))
    results = []

    for index, id_value in enumerate(collection1_ids):
        if id_value:  # Check if id_value is not an empty string
            try:
                # No conversion to float since UPC is a string
                match = collection2.find_one({'UPC': id_value})

                if match:
                    print(id_value + " present in both collections: True")
                    results.append([id_value, "Walmart", True, month_value])
                else:
                    print(id_value + " not present in both collections: False")
                    results.append([id_value, "", False, month_value])

            except Exception as e:
                print(f"Error occurred while processing id_value: {id_value}. Error: {e}")
                results.append([id_value, "Error", False, month_value])

        else:
            print("Skipping empty id_value")
            results.append([id_value, "Empty ID", False])

        if index % 100 == 0:
            print(str(index) + " Products processed out of " + str(collection1_ids_len))

    df = pd.DataFrame(results, columns=["UniqueId", "MatchedCollection", "Status", "Month"])
    df.to_excel("C:\\Users\\Abhishek\\Downloads\\output.xlsx", sheet_name=month_value, index=False)
    print("Output written")

except Exception as e:
    print(e)
