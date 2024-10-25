# Description
    # Python script to fetch the ISBN value which are acquired from walmart P4 and search them in Target data of 9 lakhs

import pymongo
import pandas as pd
import re
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["test_catalogue"]
collection_name = "TargetData_With_ISBN_Values"

excel_file_path = "D:/Python/Matched_ISBN_Records.xlsx"
df = pd.read_excel(excel_file_path)

all_matched_record = []
excel_isbn_values = []
# isbn_pattern = r'ISBN:\s*([\d\-]+)'
isbn_pattern = r'ISBN(?:10)?:?\s*([\d\-]+)'
collection = mydb[collection_name]
query = {"'ISBN_Aadu": {"$ne": ""}}
print(query)
mongo_isbn_records = list(collection.find(query))

# mongo_isbn_values = {record["'ISBN_Aadu"].replace("'", "") for record in mongo_isbn_records}
mongo_isbn_values = {record.get("'ISBN_Aadu", "").replace("'", "") for record in mongo_isbn_records}
print(f'mongo_isbn_values:{mongo_isbn_values}')
print(f'Total non-empty isbn recieved {len(mongo_isbn_values)}')

for index,row in df.iterrows():
    long_description = row["Long_Description"]
    match = re.search(isbn_pattern, long_description)
    if match:
        isbn_value = match.group(1)
        excel_isbn_values.append(isbn_value)
print(f'Excel isbn values list:{excel_isbn_values}')
print(f'Excel isbn values list length:{len(excel_isbn_values)}')

matched_isbns = set(mongo_isbn_values).intersection(excel_isbn_values)
print(f'matched_isbns:{matched_isbns}')
print(f'matched_isbns length:{len(matched_isbns)}')

if matched_isbns:
    excel_output_path = "C:/Users/Abhishek/Downloads/WMatched_ISBN_Records_Final.xlsx"
    df = pd.DataFrame(matched_isbns)
    df.to_excel(excel_output_path, index=False)
    print(f'Exported {len(matched_isbns)} matched records to "Matched_ISBN_Records"')
else:
    print("No records matched")








