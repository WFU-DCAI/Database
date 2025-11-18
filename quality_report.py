import sys, os
import pandas as pd
from datetime import datetime

#Author: Aubrie Pressley
#Date: 08/20/2025
#Purpose of this script is to check the quality of a csv file

#Report the dimensions of the data
def data_shape(data, output_file):
    with open(output_file, "a") as f:
        num_columns = data.shape[1]
        num_rows = data.shape[0]
        f.write(f"\nNumber of columns: {num_columns}\n")
        f.write(f"Number of rows: {num_rows}\n")

#Display the name of each column
#For each column, display the number of vissing values 
#For each column, display the datatype 
def display_column_names(data, report_file): 
    with open(report_file, "a") as f: 
        f.write("\nNames of each column & relevant info:\n")
        for col in data.columns: 
            f.write(f"{col}\n")
            f.write(f"   Data type: {data[col].dtype}\n") #display data type
            missing_count = data[col].isna().sum() #get the number of rows with missing values
            f.write(f"   Number of missing values: {missing_count}\n")

            #Write the range for numerical values 
            if data[col].dtype == 'int64' or data[col].dtype == 'float64':
                max_value = data[col].max()
                min_value = data[col].min()
                f.write(f"   Max value: {max_value}\n")
                f.write(f"   Min value: {min_value}\n")
            else: #display number of unique values for non numerical 
                unique_values = data[col].nunique()
                f.write(f"   Number of unique values: {unique_values}\n")
            

#Report the datatype for each column 
def data_type(data, report_file):
    with open(report_file, "a") as f:
        f.write("\nData types of columns:\n")
        f.write(f"{data.dtypes.to_string()}\n")

#Report number of duplicate Rows
def duplicate_rows(data, report_file):
    with open(report_file, "a") as f:
        duplicate_rows = data[data.duplicated()]
        f.write(f"\nDuplicate rows: {len(duplicate_rows)}\n")

#Report number of rows with missing data
def rows_missing(data, report_file):
    with open(report_file, "a") as f:
        rows_missing = data[data.isna().any(axis=1)]
        f.write(f"\nNumber of rows with missing values: {len(rows_missing)}\n")

#Report number of columns with missing values
def col_missing(data, report_file):
    with open(report_file, "a") as f:
        columns_missing = data.columns[data.isna().any(axis=0).tolist()]
        #f.write(f"Columns that have missing values: {columns_missing}\n")
        f.write(f"Number of columns with missing values: {len(columns_missing)}\n")

def main():
    args = sys.argv[1:]
    if len(args) != 1:
        print('Enter path of the csv file.')
        sys.exit(0)
    elif(not os.path.exists(args[0])):
        print('Invalid filepath')
        sys.exit(0)
    else:
        file_name = args[0]
        output_dir = os.path.join(os.path.dirname(os.path.abspath(file_name)), '..', 'output')
        output_dir = os.path.abspath(output_dir)  # normalize the path

        print(f"Running data quality report for the CSV files.")

        #get the data from the csv file
        try: 
            data = pd.read_csv(file_name, header=0)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
            return
        
        #set up the output file to save results 
        file_name_wo_csv = os.path.basename(file_name).replace('.csv', '')  # get CSV file name without extension
        output_file = os.path.join(output_dir, f"quality_report_{file_name_wo_csv}.txt")
        with open(output_file, "w") as f: #create the file
            f.write("Quality Report:\n")
        
        data_shape(data, output_file)
        duplicate_rows(data, output_file)
        rows_missing(data, output_file)
        col_missing(data, output_file)
        display_column_names(data, output_file)

        print(f"Data quality report saved to files in the output directory.")

if __name__ == "__main__":
    main()