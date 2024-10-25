# Description
    # Script to convert bunch of .xlsx, .xls files to .csv with proper encoding/ Encoding Version2
import os
import pandas as pd

# Define the directories
input_directory = r"D:/PlatformX files/Target/Converted_Excel_Files/Part1"
output_directory = r"D:/PlatformX files/Target/Converted_Excel_Files/Part1_CSV"

# Ensure the output directory exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Loop through all files in the input directory
for filename in os.listdir(input_directory):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):  # Check for Excel files
        input_filepath = os.path.join(input_directory, filename)

        # Read the Excel file
        df = pd.read_excel(input_filepath)

        # Convert the filename to the CSV format
        csv_filename = filename.rsplit('.', 1)[0] + ".csv"  # Replace .xlsx or .xls with .csv
        output_filepath = os.path.join(output_directory, csv_filename)

        # Save the DataFrame to a CSV file with UTF-8 encoding
        df.to_csv(output_filepath, encoding='utf-8', index=False)

        print(f"Converted {filename} to {csv_filename} and saved to {output_directory}")

print("All Excel files have been converted to CSV and saved.")
