# Description
    # Python Script to read the matched ISBN values from the excel find them inside the collection and bring all the data of matched records
    # Matched records are saved in an excel file

from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Adjust the connection string if needed
db = client['test_catalogue']
collection = db['TargetData_With_ISBN_Values']

filtered_data = list(collection.find({
    "'ISBN_Aadu": {'$ne': ''}
}))

df_filtered = pd.DataFrame(filtered_data)

excel_file_path = "C:/Users/Abhishek/Downloads/Matched_ISBN_Records_Final.xlsx"
df = pd.read_excel(excel_file_path)
ISBN_list = df['ISBN'].apply(lambda x: f"'{x}").tolist()
print(f'This is the isbn list:{ISBN_list}')
print(f'This is the isbn list count:{len(ISBN_list)}')

matched_data = df_filtered[df_filtered["'ISBN_Aadu"].isin(ISBN_list)]
print(f'Matched data: {matched_data}')

output_path = "C:/Users/Abhishek/Downloads/Matched_ISBN_Records_With_Data.xlsx"
matched_data.to_excel(output_path, index=False)
print(f'Exported matched records to: {output_path}')
