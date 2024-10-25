# Description
    # Python script to change encoding of CSv Files Version1
import pandas as pd

# Read the CSV file with the correct encoding (adjust encoding if necessary)
df = pd.read_csv(r"D:\PlatformX files\Target\WalmartDataOutput_Gaurav_CSV.csv", encoding='ISO-8859-1')

# Save it as a UTF-8 encoded CSV
df.to_csv(r"D:\PlatformX files\Target\WalmartDataOutput_Gaurav_CSV_EN.csv", encoding='utf-8', index=False)