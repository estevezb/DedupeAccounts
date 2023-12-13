## These python programs are modified from the orginal programs developed by Forest Gregg, DataMade , Derek Eder, DataMade. The dedupe.io app GitHub : https://github.com/dedupeio/dedupe. Also, see here for examples: https://github.com/dedupeio/dedupe-examples.
This repository modifies scripts for deduplication of duplicate records into clusters and record linkage of two datasets with similar records using the csv_example.py and record_linkage_example.py programs, respectively.

### Requirements
1) Install Python 3.8.10  for dedupe app - have not tested other versions, most recent versions will likely not work
   
example: to install specific python version 3.8.10. Correct package for python 3.8.10 is here: https://pypi.org/project/PyLBFGS/0.2.0.13/#files ;
PyLBFGS-0.2.0.13-cp38-cp38-win_amd64.whl

2.) Install the requirements.txt for dedupe app dependencies

3.) If using Visual Studio code, install IDE




### Matching accounts from different datasets
In this repository use the files containing words record_linkage.py for matching accounts between different datasets. (Tip: if you see this line in the script : linker = dedupe.RecordLink(fields) you are using the record linkage program for matching accounts in two datasets). 

### Deduplicating accounts in a dataset
In this repository use the files containing words Dedupe.py or csv_example.py for deduplicating a dataset into groups or clusters. (Tip: if you see this line in the script : deduper = dedupe.Dedupe(fields) you are using the Dedupe program for deduplicating accounts into clusters). 

### Protocol
#### Example of deduplicating accounts, creating unique IDs to label those duplicates and then fuzzy matching those accounts to a master dataset

1.) Execute the deduplication of accounts, using Dedupe_DataPond_matched_data_v4.py with modifications for file paths, file names and column names.

1a.)  If no training or settings file exists, you must do active labeling on the dataset and then these files will be created and saved.

2.)  Assign MDM ID to the clustered account results using the Assign_account_IDs_20231209.ipynb account assigning script, and making sure to base the max MDM ID by setting the path to the most recent logBook containing the Max MDM ID.

3.)  Execute the matching of clustered accounts with assigned MDM ID to a second file of accounts using the record_linkage_Spec_Distributers_v2.py

4.)  You will have 3 output files: 1) Log of records not used for fuzy matching due to lack of a usable address field, 2) the finalized matched results file ( '_paired_horizontal.csv'), and 3) complete results with matched and unmatched records

5.)  Use the file with the suffix '_paired_horizontal.csv' to see the finalized paired results

6.)  Use the file with the suffix '_{current data time}.csv' to see the complete results - matched and unmatched records - and decide on a valid cut-off Score value for marking reliable matches. 

6a.)  Note when you open the csv in excel, you will have to group the complete results file by cluster ID by using count IF on the Cluster ID column, and filter by sets having two members per clusters. 

7.)  Save as complete results file as an excel file with separate sheets for paired and unpaired results for analysis documentation - if needed.
