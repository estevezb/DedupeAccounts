import datetime
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import subprocess

logging.basicConfig(filename='watch_dog_email_file_updates.log', level=logging.INFO)

class Watcher:
    DIRECTORY_TO_WATCH = r"C:\path\to\directory\to\watch\for\changes" # add the path to the directory you want to monitor here. Use raw 'r' string to avoid issues with backslashes
    def __init__(self):
        self.observer = Observer()

    def get_state(self, directory):
        now = datetime.datetime.now()
        start_of_day = datetime.datetime(now.year, now.month, now.day, 4)  # 6AM today, change the hour here to the hour you want to start monitoring for changes
        start_of_day_timestamp = start_of_day.timestamp()
        return {os.path.join(root, f): os.stat(os.path.join(root, f)).st_mtime for root, dirs, files in os.walk(directory) for f in files if os.stat(os.path.join(root, f)).st_mtime >= start_of_day_timestamp}

    def run(self):
        print("Current time:", datetime.datetime.now())
        logging.info('Starting directory watcher')
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)  # Check for changes every 5 seconds
        except KeyboardInterrupt:
            self.observer.stop()
            print("Observer Stopped")
        self.observer.join()

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        print(f"New file {event.src_path} has been detected")
        filename = os.path.basename(event.src_path)
        if filename.startswith('Mastering Full Extract') or filename.startswith('SFDC Accounts Extract'): # add the prefix of the file you want to monitor here
            print(f"Detected new file with specified prefix: {filename}")
            time.sleep(1)  # Add a small delay
            if os.path.exists(event.src_path):  # Check if the file still exists
                self.send_email(filename, event.src_path)
                self.run_script()
                
    def run_script(self):
        python_path = r"C:/Users/beste/envs/dedupe-examples/Scripts/python.exe" # add the path to your python.exe file here. Use raw 'r' string to avoid issues with backslashes
        subprocess.run([python_path, r"C:\path\to\directory\P01_Account counting scripts\P01_check_new_accounts_against_logbook.py"]) # add the path to the script you want to run here. Use raw 'r' string to avoid issues with backslashes
        time.sleep(120)  # Wait for 2 minutes
        subprocess.run([python_path, r"C:\path\to\directory\P02_Account deduplication scripts\P02_Dedupe_new_and_historical_accounts.py"])
        time.sleep(120)  # Wait for 2 minutes
        subprocess.run([python_path, r"C:\path\to\directory\P03_Account ID assignment scripts\P03_Assign_account_IDs_by_max_in_Logbook.py"])
        print("All scripts completed.")
        

    def send_email(self, filename, filepath):
        msg = MIMEMultipart()
        msg['From'] = 'youremail@some.com' # add the email address you want to send the email from here
        msg['To'] = 'youremail@some.com' # add the email address you want to send the email to here
        msg['Subject'] = 'New file detected'
        body = f"A new file with the specified prefix has been detected:\n\nFilename: {filename}\nFilepath: {filepath}"
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587) # add the smtp server and port here, e.g. 'smtp.gmail.com', 587, 'smtp.office365.com', 587, etc., depending on your email provider
        server.starttls()
        server.login(msg['From'], 'pajg khnv itep hmxm')
        server.send_message(msg)
        server.quit()

if __name__ == '__main__': #name is a special varibale in python. when a file runs, name variable is set to main. This is to determine if the script is the main program or it is a module in another script. 
    w = Watcher() # if the script is imported as a module in another script, the code block under if__name__=='__main__': will not run. Variables and functions defined within this block will not be accessible if imported in another script, ensuring unintended execution
    w.run()