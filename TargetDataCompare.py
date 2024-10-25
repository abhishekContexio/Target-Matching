#Description
    #This python script compares UPC values from two mongoDb collections one is target data
    # The other is Walmart P4 monthwise data
    # Writes the matching records to an excel file

import pymongo
import pandas as pd
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection1 = mydb["target_data5"]
collection2 = mydb["AUG_WalmartData"]

unique_id_field = "upc"
batch_size = 10000  # Process 10,000 records at a time
output_file = "C:\\Users\\Abhishek\\Downloads\\output.xlsx"

collection2.create_index([('UPC', pymongo.ASCENDING)])

def process_batch(batch):
    results = []
    for doc in batch:
        id_value = doc.get(unique_id_field)

        if id_value and id_value != "'":  # Ignore empty or invalid values
            match = collection2.find_one({'UPC': id_value})
            if match:
                results.append([id_value, "Walmart", True])
            else:
                results.append([id_value, "", False])
        else:
            results.append([id_value, "Empty or Invalid ID", False])

    return results


try:
    myclient.admin.command("ping")
    print("MongoDB Connection established successfully")

    cursor = collection1.find({}, {unique_id_field: 1}, no_cursor_timeout=True)

    all_results = []
    batch = []
    index = 0

    # Using ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for doc in cursor:
            batch.append(doc)
            index += 1

            # If batch size is reached, process in parallel
            if len(batch) == batch_size:
                futures.append(executor.submit(process_batch, batch))
                batch = []  # Reset batch

            if index % 50000 == 0:
                print(f"Processed {index} products")

        # Process any remaining documents in the last batch
        if batch:
            futures.append(executor.submit(process_batch, batch))

        # Collect results from all threads
        for future in as_completed(futures):
            all_results.extend(future.result())

    # Convert results to DataFrame and save to Excel
    df = pd.DataFrame(all_results, columns=["UniqueId", "Matched Collection", "Status"])
    df.to_excel(output_file, index=False)
    print("Output written")

except Exception as e:
    print(e)
finally:
    cursor.close()