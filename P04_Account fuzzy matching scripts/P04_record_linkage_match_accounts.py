
"""
This code demonstrates how to use RecordLink with two comma separated
values (CSV) files. The task is to link accounts between the datasets.

The output will be a CSV with our linked results.

"""
import os
import csv
import re
import logging
import optparse
import chardet
import dedupe
from unidecode import unidecode
import time # track time for waiting for file to save before reading back into a dataframe
import pandas as pd # use to to work on data as dataframe
from datetime import datetime
import glob # In your local windows machine, .glob is used to find pathnames matching a specified pattern.





# Set up file locations

###=================================== Net new accounts data, this file contains the list of new accounts that is to be mapped to records in salesforce

# Define the base directory and the file prefix
base_directory = r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\12 Ops"
file_prefix = 'P03a_MDM_ID_assigned_net_new_accounts_only'

# Use glob to find all files that match the prefix in all subdirectories of the base directory. the ** means that it will search all subdirectories of the base directory. The *.csv means that it will search for files with the .csv extension. So, it will look for the file prefix and the .csv extension in all subdirectories of the base directory, regardless of how many levels deep they are.
files = glob.glob(os.path.join(base_directory, '**', file_prefix + '*.csv'), recursive=True) #The recursive=True argument means that it will search all subdirectories of the subdirectories, and so on, regardless of how many levels deep they are.

# If no files were found, print an error message and exit
if not files:
    print(f"No files found with prefix '{file_prefix}' in directory '{base_directory}'")
    exit(1)

# Sort the files by date, and select the most recent one
latest_Net_New_Accounts_file = max(files, key=os.path.getctime)

print(f"Using file '{latest_Net_New_Accounts_file}'") # this will print the most recent file name that will be used for the deduplication process

# Get the parent directory of the latest file
base_path = os.path.dirname(latest_Net_New_Accounts_file)

# Get the directory two levels above the current directory
base_path = os.path.dirname(os.path.dirname(base_path))

print(f"Using base path '{base_path}'")

#check if the output subdirectory exists, if not create it
if not os.path.exists(os.path.join(base_path, 'mastering')):
    os.makedirs(os.path.join(base_path, 'mastering')) # This will create a new subdirectory for the output file within base_path
    
# New subdirectory within base_path
output_subdirectory = "mastering"

# New subdirectory within base_path
Processed_input_subdirectory = "Processed_inputs"



#=================================== Salesforce accounts data, this file contains the most recent accounts in salesforce to be used for reference when deciding which account are new accounts
file_prefix2 = 'SFDC Accounts Extract'

# Use glob to find all files that match the prefix in all subdirectories of the base directory
files = glob.glob(os.path.join(base_directory, '**', file_prefix2 + '*.xlsx'), recursive=True)

# If no files were found, print an error message and exit
if not files:
    print(f"No files found with prefix '{file_prefix2}' in directory '{base_directory}'")
    exit(1)

# Sort the files by date, and select the most recent one
latest_SFDC_extract_file = max(files, key=os.path.getctime)

print(f"Using file '{latest_SFDC_extract_file}'") # this will print the full path of the most recent file that will be used for checking new accounts against the logbook

# Get the parent directory of the latest file
input3_base_path = os.path.dirname(latest_SFDC_extract_file)

print(f"Using base path '{input3_base_path}'") # this will print the base path that will be used for loading files and saving the results of the analysis of new accounts

#latest_file = os.path.join(input2_base_path, 'Mastering Full Extract 20231218_114447.xlsx')

sfdc_extract_filename = os.path.basename(latest_SFDC_extract_file) # use this to print the name of the file that will be used for checking new accounts against the logbook

# Load the data from the 'SFDC Accounts Extract' file into a DataFrame
sfdc_data = pd.read_excel(latest_SFDC_extract_file)

# Define a dictionary mapping the old column names to the new column names
column_names = {
    'SFDC ID': 'Id',
    'Address': 'ShippingStreet',
    'City': 'ShippingCity',
    'State': 'ShippingState',
    'Zip': 'ShippingPostalCode'
}

# Rename the columns
sfdc_data = sfdc_data.rename(columns=column_names)

# Save the DataFrame to a CSV file with the same name
csv_file_name = os.path.splitext(latest_SFDC_extract_file)[0] + '.csv'
sfdc_data.to_csv(csv_file_name, index=False)

print(f"Saved DataFrame to '{csv_file_name}'")


# create variable that stores today's data and time by # Generate the current date and time and then convrt it into string to use as a file name suffix in 'YYYYMMDD_HHMMSS' format
current_time_suffix= datetime.now().strftime("%Y%m%d_%H%M%S")

# Base name for the output file
base_output_filename = 'P04a_Customer_mastering_output'


#base name for the output file
output_filename= f"{base_output_filename}_All_Sources_Fuzzy_Result_{current_time_suffix}.csv"




# Get the current date and time
current_datetime = datetime.now()

# Print the current date and time
print("Current date and time:", current_datetime)
# Format the date and time
formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
# Print the formatted date and time
print("Formatted date and time:", formatted_datetime)

# use to help decode uncommon characters
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# Function to check if file exists and is not empty
def wait_for_file(filename, timeout=60):
    """
    Function to check if a file exists and is not empty.
    Args:
        filename (str): The path of the file to check.
        timeout (int): The maximum time to wait for the file, in seconds.
    Returns:
        bool: True if the file exists and is not empty, False otherwise.
    """
    start_time = time.time()
    while True:
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            return True
        elif time.time() - start_time > timeout:
            return False
        time.sleep(1)  # Wait for 1 second before checking again

#ShipTo	Address	City	State	Zip

def preProcess(column_name, column_value):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    if column_value is None:
        return ''
    if column_name =='ShippingPostalCode':
        # Remove characters after hyphen in zip code
        column_value = column_value.split('-')[0]
        # Pad zip code with leading zeros if it's less than 5 digits
        column_value = column_value.zfill(5)

    column_value = unidecode(column_value)
    column_value = re.sub('\n', ' ', column_value)
    column_value = re.sub('-', ' ', column_value)
    column_value = re.sub('\.', ' ', column_value)
    column_value = re.sub('/', ' ', column_value)
    column_value = re.sub("'", '', column_value)
    column_value = re.sub(",", ' ', column_value)
    column_value = re.sub(":", ' ', column_value)
    column_value = re.sub('  +', ' ', column_value)
    column_value = column_value.strip().strip('"').strip("'").lower().strip()
    if not column_value:
        return ''  # Return an empty string instead of None
    return column_value

#Function to save dropped records

def writeDroppedRow(row, dropped_file):
    """
    Write a dropped row (no address, no name, failed QC) to the specified output file.
    """
    with open(dropped_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if f.tell() == 0:  # Check if file is empty to write header
            writer.writeheader()
        writer.writerow(row)
        
def readData(filename, dropped_file=None):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID.
    """
    data_d = {}
    trade_partner_count = 0  # Counter for 'Trade Partner' records
    encoding = detect_encoding(filename)  # Detect the file encoding

    with open(filename,  'r', encoding=encoding) as f:
        reader = csv.DictReader(f)
        print(f"Field names in {filename}: {reader.fieldnames}")  # Print field names
        #if rename_columns:
            # Renmae the columns if needed
        #    reader.fieldnames = [rename_columns.get(name,name) for name in reader.fieldnames]
        for i, row in enumerate(reader):
            clean_row = dict([(k, preProcess(k, v)) for (k, v) in row.items()])
            
            # Drop rows with blank values for specific fields (e.g., 'ShippingStreet')
            # Adjust this condition based on your specific requirements
            if 'RecordType' in clean_row and clean_row.get('RecordType').strip().lower() == 'trade partner': #This is to skip 'Trade Partner' records. It will only work if the 'RecordType' column is present in the input file, which is not the case for the 'Processed__ExFactory Mastering Input20231208.csv' file.
                trade_partner_count += 1  # Increment the counter
                continue
            
            #if filename.endswith ('SFDC Accounts Extract 20231212_133410.csv') and not clean_row.get('ShippingStreet'):
            #   continue
            #elif filename.endswith('Processed__ExFactory Mastering Input20231208.csv'):
            #    shipping_street = clean_row.get('ShippingStreet', '')
            #    if not clean_row.get('Name') or not any(char.isdigit() for char in shipping_street) or '?' in shipping_street:
            #        if dropped_file:
            #            writeDroppedRow(row, dropped_file)
            #        continue

            data_d[filename + str(i)] = dict(clean_row)
            
    print(f"Number of 'Trade Partner' records skipped in {filename}: {trade_partner_count}")
    return data_d


if __name__ == '__main__':
    
    # ## Logging

    # dedupe uses Python logging to show or suppress verbose output. Added for convenience.
    # To enable verbose logging, run `python examples/csv_example/csv_example.py -v`
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    # ## Setup

    output_file = os.path.join(base_path,output_subdirectory,output_filename) #Set File name Customer_mastering_output_20231103, #Set File name TradePartner_mastering_output_20231103
    settings_file = 'data_matching_learned_settings_SpecDist_20231208' #these files are stored in the same directory as the script, so the path does not need to be specified
    training_file = 'data_matching_training_SpecDist_20231120_updated_3.json'

    left_file = csv_file_name # This specified the latest SFDC extract to which we are mapping accounts
    right_file = latest_Net_New_Accounts_file # Set path folder Internal Controls,
    
    # Define the path for the file to store dropped rows
    dropped_rows_file = os.path.join(base_path, output_subdirectory, f'Customer_mastering_output_Spec_Distr - LOG unusable records_{current_time_suffix}.csv')
    #Define the new colum names mapping
    #column_rename_mapping_data_2 = {
    #    'SHIP_TO_NAME' : 'Name',
    #    'ADDRESS': 'ShippingStreet',
    #    'CITY' : 'ShippingCity',
    #    'STATE' : 'ShippingState',
    #    'ZIPCODE' : 'ShippingPostalCode'
    #}
    
    print('importing data ...')
    data_1 = readData(left_file) #SF Accounts  # No renaming needed for data_1
    data_2 = readData(right_file, dropped_rows_file) # Renaming for data_2 # Data pond customer mastering input with renamed columns to match SFDC column names
    
    # New column renaming for the output file
    output_column_rename_mapping = {
        'ShippingStreet': 'Address',
        'ShippingCity': 'City',
        'ShippingState': 'State',
        'ShippingPostalCode': 'Zip'
    }
    #def descriptions():
    #    for dataset in (data_1, data_2):
    #        for record in dataset.values():
    #            yield record['description']

    # ## Training

    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
        
    else:
        # Define the fields the linker will pay attention to
        fields = [
            {'field': 'Name', 'type': 'String'},
            {'field': 'ShippingStreet', 'type': 'String', 'has missing': True}, # these fields are not needed for matching
            {'field': 'ShippingCity', 'type': 'String', 'has missing': True},
            {'field': 'ShippingState', 'type': 'Exact', 'has missing': True},
            {'field': 'ShippingPostalCode', 'type': 'String', 'has missing': True}]

        # Create a new linker object and pass our data model to it.
        linker = dedupe.RecordLink(fields)

        # If we have training data saved from a previous run of linker,
        # look for it an load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)  
            with open(training_file) as tf:
                linker.prepare_training(data_1,
                                        data_2,
                                        training_file=tf,
                                        sample_size=150000)

        else:
            linker.prepare_training(data_1, data_2, sample_size=150000)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as matches
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(linker)

        linker.train(recall=0.75, index_predicates=True) # Train the model. Can add recall parameter to prioritize precision during training by setting recall=0.5. Higher number more matches will be returned but more false positives will aslo be returned.
        
        # If you're using a pre-trained model, you might want to set a default threshold or calculate it again: 
        #threshold = linker.threshold(data_1, data_2, recall_weight=0.5) # or you can use the linker.threshold function here as well    
        
        # When finished, save our training away to disk
        with open(training_file, 'w') as tf:
            linker.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, 'wb') as sf:
            linker.write_settings(sf)

    # ## Blocking

    # ## Clustering

    # Find the threshold that will maximize a weighted average of our
    # precision and recall.  When we set the recall weight to 2, we are
    # saying we care twice as much about recall as we do precision.
    #
    # If we had more data, we would not pass in all the blocked data into
    # this function but a representative sample.
    
    # Compute the threshold
    #threshold = linker.threshold(data_1, data_2, recall_weight=0.9)
    
    print('clustering...')
    linked_records = linker.join(data_1, data_2, 0.1, 'many-to-one' ) # Many to one means that that multiple SFDC_ID in SF Id will be allowed to map to the same Q_Id Datapond account, we resolve by picking one with the SFDC Id with highest match score

    print('# duplicate sets', len(linked_records))
    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (cluster, score) in enumerate(linked_records):
        for record_id in cluster:
            cluster_membership[record_id] = {'Cluster ID': cluster_id,
                                             'Link Score': score}
    
    #Mapping all names back to the original ones used in Datapond
    #column_rename_mapping = {
    #    'ShipTo' : 'Name',
    #    'ShippingStreet' : 'Address',
    #    'ShippingCity': 'City',
    #    'ShippingState' : 'State',
    #    'ShippingPostalCode' : 'Zip'
    #}
    
    # Directly specify the encoding for the output file
    output_encoding = 'utf-8'
    
    #encoding = detect_encoding(output_file)  # Detect the file encoding
    
    with open(output_file, 'w', newline='', encoding=output_encoding, errors='replace') as f:
        header_unwritten = True

        for fileno, filename in enumerate((left_file, right_file)):
            file_encoding = detect_encoding(filename) # Detect encoding for each input file
            with open(filename, 'r', encoding=file_encoding, errors='replace') as f_input:
                reader = csv.DictReader(f_input)

                if header_unwritten:
                    #Define the column to exclude
                    columns_to_exclude = {'ShippingStreet', 'ShippingCity', 'ShippingState', 'ShippingPostalCode'} #these are duplicated and can be safely removed
                    # Predefined fieldnames
                    predefined_fieldnames = ['Cluster ID', 'Link Score', 'MDM ID', 'Dedupe_confidence_score', 'Dedupe_Cluster ID','source file', 'Id', 'Name', 'Address', 'City', 
                                            'State', 'Zip', 'Q_ID', 'Source', 'ShipTo Code', 'Count of Records', 
                                            'Sum of Quantity', 'Max of Date', 'SFDC ID - Customer', 'Transaction Bucket'
                                            ,'Record Number', 'Account | ShipTo Number', 'confidence_score', 'MDM Dup', 'Account | MDM_ID__c', 'Account | SFDC ID',
                                            'Final MDM ID', 'Qualifier', 'x.2', 'Found in Salesforce', 'ShipTo First Flag', 'x', 'Net New Accounts', 'MDM First Flag', 
                                            'MDM_ID', 'Final SFDC ID', 'x.3', 'Final - Status', 'ShipTo Dup', 'Status Key', 'New_Account', 'x.1', 'Final - SFDC', 'Final - SubStatus', 'Comments']

                    # Exclude any columns from reader.fieldnames that are already in predefined_fieldnames
                    additional_fieldnames = [name for name in reader.fieldnames if name not in predefined_fieldnames and name not in columns_to_exclude]

                    # Combine predefined fieldnames with the filtered additional fieldnames
                    fieldnames = predefined_fieldnames + additional_fieldnames

                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    header_unwritten = False

                for row_id, row in enumerate(reader):
                    record_id = filename + str(row_id)
                    cluster_details = cluster_membership.get(record_id, {})
                    row['source file'] = fileno
                    row.update(cluster_details)
                    
                    row = {output_column_rename_mapping.get(k, k): v for k, v in row.items()}
                    writer.writerow(row)
                    
    # Check if the original output file is ready
    if wait_for_file(output_file):
        # Read the original output file
        file_encoding = detect_encoding(output_file)
        try:
            data = pd.read_csv(output_file, encoding=file_encoding)
        except UnicodeDecodeError:
            print(f"UnicodeDecodeError encountered. Check the encoding of {output_file}")

        # Filter rows based on 'Cluster ID'
        cluster_counts = data['Cluster ID'].value_counts()
        valid_clusters = cluster_counts[cluster_counts >= 2].index
        paired_data = data[data['Cluster ID'].isin(valid_clusters)]

        # Initialize an empty DataFrame for the paired results
        paired_results = pd.DataFrame()

        # Process each cluster
        for cluster_id in valid_clusters:
            group = paired_data[paired_data['Cluster ID'] == cluster_id]

            if len(group) == 2:
                # Separate the two rows based on 'source file'
                row_0 = group[group['source file'] == 0]
                row_1 = group[group['source file'] == 1]

                # Drop 'source file' column as it's no longer needed
                row_0 = row_0.drop(columns=['source file'])
                row_1 = row_1.drop(columns=['source file'])

                # Prefix column names to distinguish between sources
                row_0 = row_0.add_prefix('SFDC_')
                row_1 = row_1.add_prefix('ExFactory_')

                # Merge the two rows into a single row horizontally
                merged_row = pd.concat([row_0.reset_index(drop=True), row_1.reset_index(drop=True)], axis=1)

                # Append to the paired_results DataFrame
                paired_results = pd.concat([paired_results, merged_row], ignore_index=True)

        
        # Drop columns where all values are blank (NaN)
        paired_results = paired_results.dropna(axis=1, how='all')
        
        # Define the filename for the paired results
        paired_output_file = output_file.replace('.csv', '_paired_horizontal.csv')

        # Write the paired data to a new file
        paired_results.to_csv(paired_output_file, index=False)
    else:
        print(f"File {output_file} not found or is empty after waiting.")