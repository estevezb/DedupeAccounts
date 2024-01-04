import pandas as pd # to read and write dataframes, and to manipulate data
import os # to get the current working directory, and to join file paths
import glob # to find the most recent file in a directory
import chardet # to detect the character encoding needed to read a file
import csv # to read and write csv files
from datetime import datetime # to get the current date and time
from datetime import date # to get the current date
import plotly.express as px # to create interactive plots
from openpyxl import load_workbook # to load an existing Excel file and append data to it
import re # to extract the version number from a file name
#File locations

#=================================== Historical Accounts Data: Customer Mastering LogBook file
# Define the base directory and the file prefix
base_directory = r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\08 Mastering"
file_prefix = 'Customer Mastering LogBook v'

# Use glob to find all files that match the prefix in all subdirectories of the base directory
files = glob.glob(os.path.join(base_directory, '**', file_prefix + '*.xlsx'), recursive=True)

# If no files were found, print an error message and exit
if not files:
    print(f"No files found with prefix '{file_prefix}' in directory '{base_directory}'")
    exit(1)

# Define a function to extract the version number from a file name
def get_version_number(filename):
    match = re.search(r'v(\d+\.\d+)', filename)
    if match:
        return float(match.group(1))
    else:
        return 0

# Sort the files by version number, and select the one with the highest version number
latest_mastering_logbook_file = max(files, key=get_version_number)

print(f"Using file '{latest_mastering_logbook_file}'") # this will print the full path of the most recent file that will be used for checking new accounts against the logbook

# Get the parent directory of the latest file
input1_base_path = os.path.dirname(latest_mastering_logbook_file)

print(f"Using base path '{input1_base_path}'") # this will print the base path that will be used for loading the logbook file

#raw_input1_file = os.path.join(input1_base_path, 'Customer Mastering LogBook v1.12 - Pre1218.xlsx') # Set path containing the file with previously analyzed accounts

logbook_master_data_filename = os.path.basename(latest_mastering_logbook_file)





#=================================== New Accounts Data: ExFactory and Specialty Distributor source files 
# Define the base directory and the file prefix
base_directory = r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\12 Ops"
file_prefix = 'Mastering Full Extract'

# Use glob to find all files that match the prefix in all subdirectories of the base directory
files = glob.glob(os.path.join(base_directory, '**', file_prefix + '*.xlsx'), recursive=True)

# If no files were found, print an error message and exit
if not files:
    print(f"No files found with prefix '{file_prefix}' in directory '{base_directory}'")
    exit(1)

# Sort the files by date, and select the most recent one
latest_account_extract_file = max(files, key=os.path.getctime)

print(f"Using file '{latest_account_extract_file}'") # this will print the full path of the most recent file that will be used for checking new accounts against the logbook

# Get the parent directory of the latest file
input2_base_path = os.path.dirname(latest_account_extract_file)

print(f"Using base path '{input2_base_path}'") # this will print the base path that will be used for loading files and saving the results of the analysis of new accounts

#latest_file = os.path.join(input2_base_path, 'Mastering Full Extract 20231218_114447.xlsx')

mastering_extract_filename = os.path.basename(latest_account_extract_file) # use this to print the name of the file that will be used for checking new accounts against the logbook

# New subdirectory within input2_base_path
# add a check to see if the subdirectory exists, if not create it
if not os.path.exists(input2_base_path + "/Processed_inputs"):
    os.makedirs(input2_base_path + "/Processed_inputs")
    print("Directory " , input2_base_path + "/Processed_inputs" ,  " Created ")
subdirectory = "Processed_inputs" # Set subdirectory name

# Function that automatically detects the character encoding needed to read file
def detect_file_encoding(file_path):
    with open(file_path, 'rb') as file:
        result = chardet.detect(file.read())
        return result['encoding']
    
##===================================Read in datasets ===============================================================================

#Read and process data
encoding= detect_file_encoding(latest_account_extract_file)

#Read in raw_input1_file file as a dataframe. This is the master account list against which we check the new accounts
logbook_master_data= pd.read_excel(latest_mastering_logbook_file, sheet_name='AccountMaster') # If needed use header parameter. e.g.,header = 4 , would mean that the data begins on row 5 in the input file
#Try reading the raw_input1_file file as an excel file into pandas , if not try csv file format
# Function that automatically detects the character encoding needed to read file
def detect_file_encoding(file_path):
    with open(file_path, 'rb') as file:
        result = chardet.detect(file.read())
        return result['encoding']

# Read in raw_input2_file file as a dataframe. Try reading as an Excel file, if not try CSV file format
try:
    if not os.path.exists(latest_account_extract_file):
        raise FileNotFoundError(f"The file {latest_account_extract_file} does not exist.")
    if os.path.splitext(latest_account_extract_file)[1] == '.xlsx':
        mastering_extract_data = pd.read_excel(latest_account_extract_file)
    else:
        mastering_extract_data = pd.read_csv(latest_account_extract_file, encoding=encoding)
except FileNotFoundError as fnf_error:
    print(fnf_error)
except pd.errors.ParserError:
    print(f"The file {latest_account_extract_file} is not a valid CSV or Excel file.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Split the data into two datasets based on the 'Source' column
new_ExFactory_data = mastering_extract_data[mastering_extract_data['Source'] == 'ExFactory']
new_SpecDistr_data = mastering_extract_data[mastering_extract_data['Source'] != 'ExFactory']


##===================================Format logbook dataset and identify columns needed to identify new accounts ===============================================================================



# Format 'Q_ID' column (as a string, with leading/trailing spaces removed, convert to uppercase)
logbook_master_data['Q_ID'] = logbook_master_data['Q_ID'].astype(str).str.strip().str.upper()

# Filter columns from master data
logbook_master_data_filtered = logbook_master_data[['Source', 'Q_ID', 'ShipTo Code','Final - Status']]

# Write a print statement to indicate that the logbook_master_data file is now formatted and columns are being filtered for anlaysis of new accounts
print(f"LogBook Mastering Input file {logbook_master_data_filename} is now formatted and columns are being filtered for anlaysis of new accounts")

##===================================Format mastering extract dataset source 1 ===============================================================================


# Format Q_ID column ( as a string, with leading/trailing spaced removed, convert to uppercase) in new data and filter columns from new data to only include 'Q_ID' column
new_SpecDistr_data['Q_ID'] = new_SpecDistr_data['Q_ID'].astype(str).str.strip().str.upper()
new_SpecDistr_accounts= new_SpecDistr_data[~new_SpecDistr_data['Q_ID'].isin(logbook_master_data_filtered['Q_ID'])].copy() # use copy() to avoid SettingWithCopyWarning and ensure that new_SpecDistr_accounts is a new dataframe, not a view of new_SpecDistr_data. This avoids warning when you try to add a new column to new_SpecDistr_accounts

# Identify 'Q_ID' rows in new_data that are present in master_data but have 'SF_ID' as 'Not Found'
not_found_SpecDistr_accounts = new_SpecDistr_data[new_SpecDistr_data['Q_ID'].isin (logbook_master_data_filtered[logbook_master_data_filtered['Final - Status']== 'Not Found']['Q_ID'])]

# Print statement to indicate that the new_SpecDistr_data file is now formatted and columns are being filtered for anlaysis of new accounts
print(f"New Specialty Distributor Extract file {mastering_extract_filename} is now formatted and processed for analysis of new accounts")


# Various printed results to check numbers of records in the new data and compare to historical data

unique_SpecDistr_Q_ID_count_total = new_SpecDistr_data['Q_ID'].nunique()
print(f"Total Q_ID count of unique records in current Specialty Distributor Extract {mastering_extract_filename} is: {unique_SpecDistr_Q_ID_count_total}")

# Count number of unique records for Non-'ExFactory' sources i.e., 'Specialty Distributors'
unique_specdistr_Q_ID_count_master = logbook_master_data_filtered[logbook_master_data_filtered['Source'] != 'ExFactory']['Q_ID'].nunique()


print(f"Unique Q_ID count in master historical data for 'Specialty Distributors' (all Non-ExFactory sources) in {logbook_master_data_filename} is : {unique_specdistr_Q_ID_count_master}")


new_SpecDistr_Q_ID_count_total = new_SpecDistr_accounts['Q_ID'].count()
print(f"Total Q_ID record count new Specialty Distributor accounts not in historical LogBook: {new_SpecDistr_Q_ID_count_total}")


unique_SpecDistr_Q_ID_count_new = new_SpecDistr_accounts['Q_ID'].nunique()
print(f"Unique Q_ID count in new Specialty Distributor accounts: {unique_SpecDistr_Q_ID_count_new}")


# Count for 'Q_ID' rows in new_data that are present in historical logbook master_data but have 'SF_ID' as 'Not Found'

Total_SpecDistr_Q_ID_count_not_found = not_found_SpecDistr_accounts['Q_ID'].count()
print(f"Total Historical ExFactory records Q_ID count, SF Id is not found in LogBook: {Total_SpecDistr_Q_ID_count_not_found}")

unique_SpecDistr_Q_ID_count_not_found = not_found_SpecDistr_accounts['Q_ID'].nunique()
print(f"Unique Historical ExFactory records Q_ID count, SF Id is not found in LogBook: {unique_SpecDistr_Q_ID_count_not_found}")

# Write a print statment to indicate the analysis of new accounts is complete for the new_SpecDistr_data file
print(f"Analysis of new Specialty Distributor accounts in {mastering_extract_filename} is complete")

##===================================Format mastering extract dataset source 2 ===============================================================================

# Format Q_ID column ( as a string, with leading/trailing spaced removed, convert to uppercase) in new data and filter columns from new data to only include 'Q_ID' column
new_ExFactory_data['Q_ID'] = new_ExFactory_data['Q_ID'].astype(str).str.strip().str.upper()
new_ExFactory_accounts = new_ExFactory_data[~new_ExFactory_data['Q_ID'].isin(logbook_master_data_filtered['Q_ID'])].copy() # use copy() to avoid SettingWithCopyWarning and ensure that new_ExFactory_accounts is a new dataframe, not a view of new_ExFactory_data. This avoids warning when you try to add a new column to new_ExFactory_accounts

# Identify 'Q_ID' rows in new_data that are present in master_data but have 'SF_ID' as 'Not Found'
not_found_ExFactory_accounts = new_ExFactory_data[new_ExFactory_data['Q_ID'].isin (logbook_master_data_filtered[logbook_master_data_filtered['Final - Status']== 'Not Found']['Q_ID'])]

# Print statement to indicate that the new_SpecDistr_data file is now formatted and columns are being filtered for anlaysis of new accounts
print(f"New ExFactory Extract file {mastering_extract_filename} is now formatted and processed for analysis of new accounts")


# Various printed results to check numbers of records in the new data and compare to historical data


unique_ExFactory_Q_ID_count_total = new_ExFactory_data['Q_ID'].nunique()
print(f"Total Q_ID count of unique records in current ExFactory Extract {mastering_extract_filename} is: {unique_ExFactory_Q_ID_count_total}")

# Count number of unique records for 'ExFactory' source
unique_exfactory_Q_ID_count_master = logbook_master_data_filtered[logbook_master_data_filtered['Source'] == 'ExFactory']['Q_ID'].nunique()

print(f"Unique Q_ID count in master historical data for 'ExFactory' source in {logbook_master_data_filename} is : {unique_exfactory_Q_ID_count_master}")

# These are the total number of new account records that needed to be mastered
new_ExFactory_Q_ID_count_total = new_ExFactory_accounts['Q_ID'].count()

print(f"Total Q_ID record count new ExFactory accounts not in historical LogBook: {new_ExFactory_Q_ID_count_total}")

# These are the unique number of new account records that needed to be mastered
unique_ExFactory_Q_ID_count_new = new_ExFactory_accounts['Q_ID'].nunique()

print(f"Unique Q_ID record count new ExFactory accounts not in historical LogBook: {unique_ExFactory_Q_ID_count_new}")


# Count for 'Q_ID' rows in new_data that are present in historical logbook master_data but have 'SF_ID' as 'Not Found'

Total_ExFactory_Q_ID_count_not_found = not_found_ExFactory_accounts['Q_ID'].count()
print(f"Total Historical ExFactory records Q_ID count, SF Id is not found in LogBook: {Total_ExFactory_Q_ID_count_not_found}")

unique_ExFactory_Q_ID_count_not_found = not_found_ExFactory_accounts['Q_ID'].nunique()
print(f"Unique Historical ExFactory records Q_ID count, SF Id is not found in LogBook: {unique_ExFactory_Q_ID_count_not_found}")

# Write a print statment to indicate the analysis of new accounts is complete for the new_SpecDistr_data file
print(f"Analysis of new ExFactory accounts in {mastering_extract_filename} is complete")


##=================================== Combine the new and historical Not Found by Source ===============================================================================

# create variable that stores today's data and time by # Generate the current date and time and then convrt it into string to use as a file name suffix in 'YYYYMMDD_HHMMSS' format
current_time_suffix= datetime.now().strftime("%Y%m%d_%H%M%S")

# Concatenate the dataframes
combined_df = pd.concat([new_ExFactory_accounts, not_found_ExFactory_accounts])

# Add a new column 'new_account' and set its value to 'True' for new accounts
combined_df['new_account'] = combined_df['Q_ID'].isin(new_ExFactory_accounts['Q_ID'])

# Concatenate the dataframes
combined_SpecDistr_df = pd.concat([new_SpecDistr_accounts, not_found_SpecDistr_accounts])

# Add a new column 'new_account' and set its value to 'True' for new accounts
combined_SpecDistr_df['new_account'] = combined_SpecDistr_df['Q_ID'].isin(new_SpecDistr_accounts['Q_ID'])

# Add a new column 'Status' to the 'combined_df' dataframe
combined_df['Status'] = combined_df['new_account'].apply(lambda x: 'Not Found' if x == False else 'Pending')

# Add a new column 'Status' to the 'combined_SpecDistr_df' dataframe
combined_SpecDistr_df['Status'] = combined_SpecDistr_df['new_account'].apply(lambda x: 'Not Found' if x == False else 'Pending')

#Write a print statement to indicate that the new and historical data has been combined
print(f"New and historical data has been combined for {mastering_extract_filename}")

##=================================== OPTIONAL : Save the full results - includes both new and not found accounts- to an Excel workbook ===============================================================================

# Save the combined dataframe and the blank dataframe to an Excel file
#with pd.ExcelWriter(os.path.join(input2_base_path, subdirectory, f'New and not found accounts for Customer Mastering__{current_time_suffix}.xlsx')) as writer:
#    combined_df.to_excel(writer, sheet_name='ExFactory', index=False)
#    combined_SpecDistr_df.to_excel(writer, sheet_name='Specialty Distributors', index=False)

#print('Analysis completed and new and historical Not Found accounts saved')


##=================================== Save only the new accounts to csv file for streamlined downstream processing ===============================================================================

# Concatenate the new account dataframes
new_accounts_df = pd.concat([new_ExFactory_accounts, new_SpecDistr_accounts])

# Get today's date
today = date.today()

# Format the date in 'MM/DD/YYYY' format
formatted_date = today.strftime("%m/%d/%Y")

# Add a new column to the DataFrame
new_accounts_df['Date_New_Accounts_Received'] = formatted_date

print(f'There are {len(new_accounts_df)} new accounts in the {mastering_extract_filename} whose Q_ID does not exist in the {logbook_master_data_filename} on {current_time_suffix}')

# Save the dataframe to a CSV file
new_accounts_df.to_csv(os.path.join(input2_base_path, subdirectory, f'New accounts__{current_time_suffix}.csv'), index=False)

print('New accounts saved to CSV file')

##=================================== Save an updated logbook with new accounts appended to csv file for downstream processing ===============================================================================

# check if the logbook_master_data is a DataFrame
#print(type(logbook_master_data))

#Add a new column to the DataFrame, this will track new accounts
new_ExFactory_accounts['New_Account'] = True
new_SpecDistr_accounts['New_Account'] = True

# Add a new column to the DataFrame, this will track new accounts which are not found in the logbook
logbook_master_data['New_Account'] = False

# Append new accounts to the logbook master data
logbook_master_data = pd.concat([logbook_master_data, new_ExFactory_accounts, new_SpecDistr_accounts], ignore_index=True)

# Add 'Record Number' column as the first column with values starting at 1
logbook_master_data.insert(0, 'Record Number', range(1, 1+len(logbook_master_data)))

# Rename 'ShipTo' column to 'Name' to match the downstream processing script
logbook_master_data = logbook_master_data.rename(columns={'ShipTo': 'Name'})

# Save the DataFrame to a CSV file
logbook_master_data.to_csv(os.path.join(input2_base_path, subdirectory, f'Processed_new_and_historical_acc__{current_time_suffix}.csv'), index=False)

print(f'There are {len(logbook_master_data)} total records including New and historical accounts saved to CSV file')