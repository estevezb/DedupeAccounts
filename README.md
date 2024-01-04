## These python programs are modified from the dedupe.io app developed by Forest Gregg, DataMade , Derek Eder, DataMade. The dedupe.io app GitHub : https://github.com/dedupeio/dedupe. Also, see here for examples: https://github.com/dedupeio/dedupe-examples.
This repository modifies scripts for deduplication of duplicate records into clusters and record linkage of two datasets with similar records using the csv_example.py and record_linkage_example.py programs, respectively.

### Requirements
1) Install Python 3.8.10  for dedupe app - have not tested other versions, most recent versions will likely not work
   
example: to install specific python version 3.8.10. Correct package for python 3.8.10 is here: https://pypi.org/project/PyLBFGS/0.2.0.13/#files ;
PyLBFGS-0.2.0.13-cp38-cp38-win_amd64.whl

2.) Install the requirements.txt for dedupe app dependencies

3.) If using Visual Studio code, install IDE . https://code.visualstudio.com/download


### Protocol 
Note: (each script is prefixed with 'P0(value)_name_of_script_to_run.py', the number is the sequence in which the script should be executed i.e., P01 is first)

#### Overview of program execution sequence going left to right: P01 (P01_check_new_accounts_against_logbook.py) --> P02 (P02_Dedupe_new_and_historical_accounts.py)--> P03 (P03_Assign_account_IDs_by_max_in_Logbook.py)--> P04 (P04_record_linkage_match_accounts.py) STOP! :stop_sign: 	:eyes: Before executing final script (P04b), manual analysis of P04 file output is needed to generate input for P04b (P04b_fuzzy_on_threshold_matches.py). 


### Semi-Automated account processing: 
#### To have the first 3 scripts automatically triggered, use watch_dog_TEMPLATE.py to monitor the incoming files. Modify accordingly to receive email updates as well.
