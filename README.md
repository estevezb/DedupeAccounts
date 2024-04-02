## These python programs are modified from the dedupe.io app developed by Forest Gregg, DataMade , Derek Eder, DataMade. The dedupe.io app GitHub : https://github.com/dedupeio/dedupe. Also, see here for examples: https://github.com/dedupeio/dedupe-examples.
This repository modifies scripts for deduplication of duplicate records into clusters and record linkage of two datasets with similar records using the csv_example.py and record_linkage_example.py programs, respectively.

### Requirements
1.) Install Python 3.8.10  for dedupe app - have not tested other versions, most recent versions will likely not work. To install specific python version 3.8.10. see here: https://www.python.org/downloads/release/python-3810/

2.) Download and install dedupe app dependencies(click on the the requirements.txt file name, download and install globally to any scripts folder or install in a specific venv used only for this project)
- example for installing requirements open the integrated terminal in Visual studio code (or your IDE) OR command prompt terminal then do the following :
-  Navigate to the path where the requirements.txt is downloaded, and then  install requirements:
-         cd path_to_your_project
-         install -r requirements.txt

3.) If using Visual Studio code (highly recommend), install IDE . https://code.visualstudio.com/download


### Protocol for intake of new accounts, deduplication against historical accounts, new account ID assignment and fuzzy matching new accounts to a master dataset
Note: (each script is prefixed with 'P0(value)_name_of_script_to_run.py', the number is the sequence in which the script should be executed i.e., P01 is first)

#### Overview of program execution sequence: 
- P01 (P01_check_new_accounts_against_logbook.py)
-  P02 (P02_Dedupe_new_and_historical_accounts.py)
-  P03 (P03_Assign_account_IDs_by_max_in_Logbook.py)
-  P04 (P04_record_linkage_match_accounts.py) STOP! :stop_sign: 	:eyes:
-  (Optional) An additional fuzzy matching of P04 results, focusing on the borderline (potential mis-matches), final script (P04b), requiring manual analysis of P04 output to generate input for P04b (P04b_fuzzy_on_threshold_matches.py). 


### Semi-Automated account processing: 
#### To have the first 3 scripts automatically triggered, do any of the following: 
- a)use watch_dog_TEMPLATE.py script to monitor the incoming files on your local machine. Modify accordingly to receive email updates as well.
- b)OR use subprocess to run the next script ( must import subprocess)
      - e.g.,
```
         # Your P04 script would have already executed above this line
         
         # Then subprocess is used to trigger the follow-up script for analysis of threshold fuzzy matches
          print("Triggering the final script,  P04b_fuzzy_on_threshold_matches.py...")
          python_path = r"C:/Users/beste/envs/dedupe-examples/Scripts/python.exe"
          script_path = r"C:\Users\beste\OneDrive - Qral Group\Desktop\python\FuzzyMatch\P04_Account fuzzy matching scripts\P04b_fuzzy_on_threshold_matches.py 
          subprocess.run([python_path, script_path]) 
