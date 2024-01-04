import pandas as pd
import os


from datetime import datetime

#File locations

#=================================== Deduplicated new accounts data, this file contains the list of accounts from a SpecDistr Mastering Extract file that has been deduplicated i.e., grouped into clusters


input2_base_path =r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\12 Ops\20231128_A1\02 Internal Controls\mastering" #update path if needed

raw_input1_file = os.path.join(input2_base_path, 'Dedupped__SpecDist Mastering Input20231128_175947.csv') # Set path containing the file with previously analyzed accounts



#Read in raw_input1_file csv file as a dataframe. This is the dedupped SpecDistr output file
deduped_input_data= pd.read_csv(raw_input1_file)

#=================================== Define the output file path and filename



# Get the filename from the input file path
input_filename = os.path.basename(raw_input1_file)

# Add the prefix and suffix to the filename
output_filename = f"Processed__{input_filename.replace('.csv', '')}.csv"


# Create the full path for the output file
output_file = os.path.join(input2_base_path, output_filename)

#=================================== Historical Accounts Data: Customer Mastering LogBook file


input2_base_path =r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\08 Mastering\00 Logbook"

raw_input2_file = os.path.join(input2_base_path, 'Customer Mastering LogBook v0.91_be.xlsx') # Set path containing the file with previously analyzed accounts with MDM ID already assigned

log_book_filename = os.path.basename(raw_input2_file) #extract filename of logbook from path for use in print statements

#Read in raw_input1_file file as a dataframe. This is the master logbook historical account list from a EXCEL workbook against which we check the new accounts
logbook_master_data = pd.read_excel(raw_input2_file, sheet_name='AccountMaster', header=3)


#=================================== Set up MDM ID for new accounts: starting from the max MDM ID in the historical accounts data, increment by 1 and assign to each new account


# Filter out rows where 'MDM ID' is 'Not Found'
filtered_logbook_master_data = logbook_master_data[logbook_master_data['MDM ID'] != 'Not Found']

# Check the filtered_logbook_master_data dataframe for the max MDM ID value
max_mcm_id = filtered_logbook_master_data['MDM ID'].max()

# Print the max MDM ID value in the filtered_logbook_master_data dataframe as an f string to check that the max MDM ID value is correct
print(f"Max MDM ID value in historical {log_book_filename} is : {max_mcm_id}")

# Extract the numeric part of the max MDM ID and increment it by 1
if pd.isnull(max_mcm_id):
    start_id = 1
else:
    start_id = int(max_mcm_id.replace("EMUSHCO", "")) + 1
    

 # Add a print statement to check the start_id value as an f string
print(f"The starting MDM ID value for new accounts is: {start_id}")
    
# Generate unique account IDs
deduped_input_data['MDM ID'] = range(1, len(deduped_input_data)+1)  # range accepts a start and stop value. Use 1 to start numbering at 1 and use len to count up to the number of rows in the data. Add 1 because range excludes last row/stop value

# to define a fixed length of leading zeroes
id_length = 7


# Generate a unique set of 'Cluster ID' values
unique_cluster_ids = deduped_input_data['Cluster ID'].unique()

    

# Create a dictionary to map each unique 'Cluster ID' to a unique 'MCM_ID'
cluster_id_to_mcm_id = {cluster_id: f"EMUSHCO{i:0{id_length}d}" for i, cluster_id in enumerate(unique_cluster_ids, start=start_id)}

# Add a print statement to show the full MDM ID start value
print(f"The starting MDM ID value for new accounts is: EMUSHCO{start_id:0{id_length}d}")

# Map the 'MDM ID' back to the original DataFrame
deduped_input_data['MDM ID'] = deduped_input_data['Cluster ID'].map(cluster_id_to_mcm_id)

# Print the max MDM ID value in the deduped_input_data dataframe as an f string to check that the max MDM ID value is correct
print(f"Max MDM ID value in deduped_input_data is: {deduped_input_data['MDM ID'].max()}")


###================================== QC Check that the number of unique 'MDM ID' values is equal to the number of unique 'Cluster ID' values


# Get the number of unique 'Cluster ID'
num_unique_cluster_id = deduped_input_data['Cluster ID'].nunique()

# Get the number of unique 'MDM ID'
num_unique_mdm_id = deduped_input_data['MDM ID'].nunique()

# Print the number of unique 'Cluster ID' and 'MDM ID'
print(f"Number of unique Cluster ID: {num_unique_cluster_id}")
print(f"Number of unique MDM ID: {num_unique_mdm_id}")

# Get a boolean series where True indicates the 'MDM ID' is duplicated
duplicated_mdm_id = deduped_input_data.duplicated('MDM ID', keep=False) # the duplicated method returns a boolean series where True indicates the value is duplicated. The keep=False argument indicates that all duplicated values should be marked as True

# Create a new DataFrame that only includes the rows with duplicated 'MDM ID' values
duplicated_mdm_id_df = deduped_input_data[duplicated_mdm_id]

# Sort the DataFrame by 'MDM ID'
sorted_duplicated_mdm_id_df = duplicated_mdm_id_df.sort_values('MDM ID')

# Print the sorted DataFrame
print(sorted_duplicated_mdm_id_df)

#print an f string that QC checks are complete
print(f"QC checks are complete")

### Reorder the columns in the deduped_input_data dataframe to bring MDMD ID to the front

# Get a list of all columns
cols = list(deduped_input_data.columns)

# Remove 'MDM ID' from the list
cols.remove('MDM ID')

# Create a new list with 'MDM ID' at the start
cols = ['MDM ID'] + cols

# Reindex the DataFrame with the new column order
deduped_input_data = deduped_input_data[cols]


###==================================Save the data to CSV


# Save the data to CSV, overwrite the existing file
deduped_input_data.to_csv(output_file, index=False)

print(f'MDM ID added to accounts and saved as a CSV file in the same folder as the input file as {output_filename}')


