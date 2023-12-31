"""
This code demonstrates how to use RecordLink a Dedupe.io Git of Deduplication applications developed by the folks at DataMade
Is uses two comma separated values (CSV) files and links similar accounts between the datasets. 
In this version, we are trying to map an internally maintained database of customer accounts to corresponding IDs present in a salesforce customer account database.
There are 3 outputs from this script: one is the complete output file with all the columns from both input files, the second is a file with dropped records, and the third is a file with only the linked results.
The linked result output file is paired horizontally, meaning that the columns from the two input files are merged into a single row.

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

# Base path where files are located , change this to point to the correct folder on your local machine:
base_path = r"C:\Add\Path\Here"

# New subdirectory within base_path
output_subdirectory = "Add subdirectory name here"
# New subdirectory within base_path
Procossed_input_subdirectory = "Add subdirectory name here"

# use to help decode uncommon characters
def detect_encoding(file_path):
    """
    Detect the encoding of a file.
    Args:
        file_path (str): The path of the file to detect the encoding of.
    Returns: 
        (str): The encoding of the file.
    """
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

def preProcess(column_name, column_value):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    Args: 
        column_name (str): The name of the field to clean.  
        column_value (str): The value of the field to clean.    
    Returns:
        str: The cleaned field value.   
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
    column_value = re.sub(",", '', column_value)
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
    Args:
        row (dict): A dictionary representing a row of data. The keys are the column names and the values are the column values.
        dropped_file (str): The path of the file to write the dropped row to. The file will be created if it doesn't exist.
    Returns:
        None 
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
    Args:
        filename (str): The path of the CSV file to read.
        dropped_file (str): The path of the file to write dropped rows to. The file will be created if it doesn't exist.
    Returns:
        dict: The data contained in the CSV file, where the key is a unique record ID and the value is a dictionary representing a row of data. The keys are the column names and the values are the column values.
    
    """
    data_d = {}
    encoding = detect_encoding(filename)  # Detect the file encoding

    with open(filename, encoding=encoding ) as f:
        reader = csv.DictReader(f)
        #if rename_columns:
            # Renmae the columns if needed
        #    reader.fieldnames = [rename_columns.get(name,name) for name in reader.fieldnames]
        for i, row in enumerate(reader):
            clean_row = dict([(k, preProcess(k, v)) for (k, v) in row.items()])
            
            # Drop rows with blank values for specific fields (e.g., 'ShippingStreet')
            # Adjust this condition based on your specific requirements
            if filename.endswith ('Add left file here') and not clean_row.get('Add any column here to check for blank values'):
                continue
            elif filename.endswith('Add right file here/'):
                shipping_street = clean_row.get('Add  any or same column here as above to check for blank values', '')
                if not clean_row.get('Name') or not any(char.isdigit() for char in shipping_street) or '?' in shipping_street:
                    if dropped_file:
                        writeDroppedRow(row, dropped_file)
                    continue

            data_d[filename + str(i)] = dict(clean_row)

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

    ##=================================================Setup your input files and output files here

    output_file = os.path.join(base_path,output_subdirectory,'Add the name of your output file') 
    settings_file = 'data_matching_learned_settings' #these files are stored in the same directory as the script, so the path does not need to be specified
    training_file = 'data_matching_training.json' # this file is generated after the user has manually labeled the first set of records and saved the file

    left_file = os.path.join(base_path, 'Add the name of your input "master" file') # This file is the master file that you want to match to the other file, contains the most complete data
    right_file = os.path.join(base_path, Procossed_input_subdirectory, 'Add the name of your input "incomplete" or second dataset file') # This file is the second file that you want to match to the master file, contains incomplete data
    
    # Define the path for the file to store dropped rows
    dropped_rows_file = os.path.join(base_path, output_subdirectory, 'Customer_mastering_output_ExFactory - LOG unusable records_20231117.csv')

    
    print('importing data ...')
    data_1 = readData(left_file) #SF Accounts  # No renaming needed for data_1
    data_2 = readData(right_file, dropped_rows_file) # Renaming for data_2 # Data pond customer mastering input with renamed columns to match SFDC column names
    
    # New column renaming for the output file. Change these column names as needed
    output_column_rename_mapping = {
        'ShippingStreet': 'Address',
        'ShippingCity': 'City',
        'ShippingState': 'State',
        'ShippingPostalCode': 'Zip'
    }

    ##================================================================== Training

    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as sf:
            linker = dedupe.StaticRecordLink(sf)
        
    else:
        # Define the fields the linker will pay attention to
        fields = [
            {'field': 'Name', 'type': 'String'},
            {'field': 'ShippingStreet', 'type': 'String', 'has missing': True},
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
                                        sample_size=150000) # Default sample size is 10000, increase this if you have more data. Note time to train will increase with sample size

        else:
            linker.prepare_training(data_1, data_2, sample_size=150000)

        ##==================================================== Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as matches
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(linker)

        linker.train(recall=0.75, index_predicates=True) # Train the model. Can add recall parameter to prioritize precision during training by setting recall=0.5. Higher number more matches will be returned but more false positives will aslo be returned.
         
        
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
    
    print('clustering...')
    linked_records = linker.join(data_1, data_2,0.1 , 'many-to-one') # Many to one means that that multiple SFDC_ID in SF Id will be allowed to map to the same Q_Id Datapond account, we resolve by picking one with the SFDC Id with highest match score

    print('# duplicate sets', len(linked_records))
    
    
    ##====================================================  Writing Results

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
                    predefined_fieldnames = ['Cluster ID', 'Link Score', 'source file', 'Id', 'Name', 'Address', 'City', 
                                            'State', 'Zip', 'Q_ID', 'Source', 'ShipTo Code', 'Count of Records', 
                                            'Sum of Quantity', 'Max of Date', 'SFDC ID - Customer', 'Transaction Bucket']

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