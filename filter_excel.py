import pandas as pd

# Function to read Excel files and filter data
def filter_and_save(file1, sheet1, file2, sheet2, output_file):
    df1 = pd.read_excel(file1, sheet_name=sheet1)
    df2 = pd.read_excel(file2, sheet_name=sheet2)
    
    # Convert both columns to lowercase for case-insensitive filtering
    filtered_df = df2[~df2['FullName'].astype(str).str.split('.').str[0].str.strip().str.lower().isin(df1['file_name'].astype(str).str.split('.').str[0].str.strip().str.lower())]

    # Save filtered data to Excel
    filtered_df.to_excel(output_file, index=False)
    print(f'File information has been written to {output_file}')

# File paths and sheet names
file_master = 'D:\\SONG\\Master VOD 20250303 (VOD2).xlsx'
sheet_master = 'Song'

# Process first set of files
filter_and_save('D:\\SONG\\Book1.xlsx', 'Sheet1', file_master, sheet_master, 'D:\\SONG\\20250324_filtered_data_OKE.xlsx')

# Process second set of files
filter_and_save('D:\\SONG\\Book2.xlsx', 'Sheet1', file_master, sheet_master, 'D:\\SONG\\20250324_filtered_data.xlsx')

# filter_and_save('D:\\SONG\\20250324_filtered_data.xlsx', 'Sheet1', file_master, sheet_master, 'D:\\SONG\\20250324_filtered_data_v2.xlsx')
