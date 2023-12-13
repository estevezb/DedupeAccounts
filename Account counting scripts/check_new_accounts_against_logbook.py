import pandas as pd
import os
import chardet
import csv
from datetime import datetime
import plotly.express as px
import pandas as pd
from openpyxl import load_workbook

#File locations

#=================================== Historical Accounts Data: Customer Mastering LogBook file
input1_base_path =r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\08 Mastering\00 Logbook"

raw_input1_file = os.path.join(input1_base_path, 'Customer Mastering LogBook v0.9.xlsx') # Set path containing the file with previously analyzed accounts
logbook_master_data_filename = os.path.basename(raw_input1_file)





#=================================== New Accounts Data: ExFactory and Specialty Distributor source files 
input2_base_path =r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\12 Ops\20231208 - postfix\02 Internal Controls"

raw_input2_file = os.path.join(input2_base_path, 'Mastering Full Extract 20231208_163411.csv')
mastering_extract_filename = os.path.basename(raw_input2_file)

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
encoding= detect_file_encoding(raw_input2_file)

#Read in raw_input1_file file as a dataframe. This is the master account list against which we check the new accounts
logbook_master_data= pd.read_excel(raw_input1_file, sheet_name='AccountMaster', header=3) # If needed use header parameter. e.g.,header = 4 , would mean that the data begins on row 5 in the input file

# Read in the CSV file as a dataframe
mastering_extract_data = pd.read_csv(raw_input2_file, encoding=encoding)

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
new_SpecDistr_accounts= new_SpecDistr_data[~new_SpecDistr_data['Q_ID'].isin(logbook_master_data_filtered['Q_ID'])]

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
new_ExFactory_accounts = new_ExFactory_data[~new_ExFactory_data['Q_ID'].isin(logbook_master_data_filtered['Q_ID'])]

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

##=================================== Save the results to Excel workbook ===============================================================================

# Save the combined dataframe and the blank dataframe to an Excel file
with pd.ExcelWriter(os.path.join(input2_base_path, subdirectory, f'New and not found accounts for Customer Mastering__{current_time_suffix}.xlsx')) as writer:
    combined_df.to_excel(writer, sheet_name='ExFactory', index=False)
    combined_SpecDistr_df.to_excel(writer, sheet_name='Specialty Distributors', index=False)

print('Analysis completed and new and historical Not Found accounts saved')