#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](mysql_example.html)
"""

import os
import glob # for file handling. This will allow us to find the most recent file in a directory
import csv
import re
import logging
import optparse

import sys # for system library
import signal # track ctrl + c press to ask for confirmation before exit 
from datetime import datetime # for date and time library
import dedupe
from unidecode import unidecode
import multiprocessing

# Define the base directory and the file prefix
base_directory = r"C:\Users\beste\OneDrive - Qral Group\01 Narcan\12 Ops"
file_prefix = 'P01b_Processed_new_and_historical_acc__'

# Use glob to find all files that match the prefix in all subdirectories of the base directory
files = glob.glob(os.path.join(base_directory, '**', file_prefix + '*.csv'), recursive=True)

# If no files were found, print an error message and exit
if not files:
    print(f"No files found with prefix '{file_prefix}' in directory '{base_directory}'")
    exit(1)

# Sort the files by date, and select the most recent one
latest_file = max(files, key=os.path.getctime)

print(f"Using file '{latest_file}'") # this will print the most recent file name that will be used for the deduplication process

# Get the parent directory of the latest file
base_path = os.path.dirname(latest_file)

print(f"Using base path '{base_path}'") # this will print the base path that will be used for loading files and saving the results of the deduplication process

# OPTIONAL: New subdirectory within base_path
#subdirectory = "Processed_inputs"
#full_subdirectory_path = os.path.join(base_path, subdirectory)

# Check if the subdirectory exists, create it if it doesn't
#if not os.path.exists(full_subdirectory_path):
#    os.makedirs(full_subdirectory_path)

# New subdirectory within base_path
output_subdirectory = "Dedupped_Results"
full_output_subdirectory_path = os.path.join(base_path, output_subdirectory)

# Check if the output subdirectory exists, create it if it doesn't
if not os.path.exists(full_output_subdirectory_path):
    os.makedirs(full_output_subdirectory_path)

# Base name for the output file
base_output_filename = 'P02_Dedupped_Results_'

# create variable that stores today's data and time by # Generate the current date and time and then convrt it into string to use as a file name suffix in 'YYYYMMDD_HHMMSS' format
current_time_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

#base name for the output file
output_filename = f"{base_output_filename}_{current_time_suffix}.csv"

# Define a flag to track if the user wants to exit
exit_requested = False

def signal_handler(sig, frame):
    # Check if the signal is Ctrl+C
    if sig == signal.SIGINT:
        # Prompt the user to confirm exit
        confirm = input("\nAre you sure you want to exit? (y/n): ").strip().lower()
        if confirm == "y":
            print("Exiting...")
            exit(0)  # Exit the program with a successful status code
        else:
            # Continue execution if the user entered anything other than 'y'
            pass

# Set the Ctrl+C signal handler
signal.signal(signal.SIGINT, signal_handler)

def preProcess(column_name, column_value):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    if column_name == 'Zip':
        # Remove characters after hyphen in zip code
        column_value = column_value.split('-')[0]
        # Pad zip code with leading zeros if it's less than 5 digits
        column_value = column_value.zfill(5)

    column_value = unidecode(column_value)
    column_value = re.sub('  +', ' ', column_value)
    column_value = re.sub('\n', ' ', column_value)
    column_value = column_value.strip().strip('"').strip("'").lower().strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column_value:
        column_value = None
    return column_value


def readData(filename, rename_columns=None):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        if rename_columns:
            # Rename the columns if needed
            reader.fieldnames = [rename_columns.get(name, name) for name in reader.fieldnames]
        for row in reader:
            # Skip rows with blank 'Address' for 'source file' value '0'
            #if row['source file'] == '0' and not row['Address'].strip(): # Use this if your file has source files labeled as 0 (SFDC file) and 1  (ExFactory) to drop only record in source file 0 if they are blank and not in source file 1.
            # Skip rows with blank shipping street
            #if not row['Address'].strip(): # Use this if your file has source files labeled as 0 (SFDC file) and 1  (ExFactory) to drop only record in source file 0 if they are blank and not in source file 1.
            #    continue # If you only have one source file, use this line to drop all blank records. 

            clean_row = [(k, preProcess(k, v)) for (k, v) in row.items()]
            row_id = int(row['Record Number'])  # Ensure this column contains numeric values only
            data_d[row_id] = dict(clean_row)
    return data_d


if __name__ == '__main__':

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. This
    # code block lets you change the level of loggin on the command
    # line. You don't need it if you don't want that. To enable verbose
    # logging, run `python examples/csv_example/csv_example.py -v`
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

    input_file = os.path.join(base_path, latest_file)
    output_file = os.path.join(base_path, output_subdirectory,output_filename)
    settings_file = 'data_matching_learned_settings_Dedup_ExFactory_20231219' # Do not need this when run first time on your own data (can delete if exists, or if you want to repeat or add to the training file)
    training_file = 'data_matching_training_Updated_Dedup_ExFactory_20231218.json' # Change if needed to the relevant training file for the input data
    #Optional: if your input has different column names, change them here
    #column_rename_mapping = {
    #    'Name' : 'Name',
    #    'Address': 'ShippingStreet',
    #    'City' : 'ShippingCity',
    #    'State' : 'ShippingState',
    #    'Zip' : 'ShippingPostalCode'
    #}
    print('importing data ...')
    data_d = readData(input_file) # if your input data has different column names change them here :data_d = readData(input_file, rename_columns=column_rename_mapping) 

    # If a settings file already exists, we'll just load that and skip training
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # ## Training

        # Define the fields dedupe will pay attention to
        fields = [
            {'field': 'Name', 'type': 'String'},
            {'field': 'Address', 'type': 'String','has missing': True},#For fields with 'has missing': True, if one or both of the compared records have a missing value for that field, it will not prevent the deduplication process from considering them as potential duplicates
            {'field': 'City', 'type': 'String', 'has missing': True},
            {'field': 'State', 'type': 'Exact','has missing': True},
            {'field': 'Zip', 'type': 'String', 'has missing': True},
            ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)

        # If we have training data saved from a previous run of dedupe,
        # look for it and load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file, 'rb') as f:
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(data_d)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(deduper)

        # Using the examples we just labeled, train the deduper and learn
        # blocking predicates
        deduper.train()

        # When finished, save our training to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

    # ## Clustering

    # `partition` will return sets of records that dedupe
    # believes are all referring to the same entity.

    print('clustering...')
    clustered_dupes = deduper.partition(data_d, 0.5)

    print('# duplicate sets', len(clustered_dupes))

    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score
            }

    with open(output_file, 'w', newline= '') as f_output, open(input_file) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ['Cluster ID', 'confidence_score'] + reader.fieldnames

        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row_id = int(row['Record Number'])
            if row_id in cluster_membership:
                row.update(cluster_membership[row_id])
                writer.writerow(row)
