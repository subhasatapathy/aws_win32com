# -*- coding: utf-8 -*-
"""
Spyder Editor

CRR Upload Main

(c) 2024, Subham
"""
import os
import time
import shutil
import logging
from datetime import date
from aws_connect import Aws_I_O
from send_mail_outlook import send_mail
from utils import upload_download_match
from data_ops import get_balance, write_data_csv
from read_outlook import create_outlook_object, get_new_reports, read_attachment

logging.basicConfig(level=logging.INFO, filename=f'crr_upload_{date.today().strftime("%d_%m_%Y")}.log',
                    filemode='w', format='%(levelname)s:%(message)s')
log = logging.getLogger(__name__)

# Beginning of the Code with Time Captured
start_time = time.time()

# Constants
root_directory = 'D:/Python/crr_upload'
attachment_storage = os.path.join(root_directory, "Attachments")
sub_folder = "CRR Balance"
attachment_shared_storage = r"T:\Data\Power BI\Liquidity Metrics\CRR"

# Create Outlook Message Object
message_object = create_outlook_object(sub_folder)

# Get name of New Reports to be uploaded
new_reports_name = get_new_reports(message_object)

# New Reports Read as DataFrames
list_new_reports_read = read_attachment(message_object, new_reports_name,
                                        attachment_storage)

# Filter 1: Get the dates and CRR balances from Outlook for which send receipts are found
date_value = []
for value in list_new_reports_read:
    try:
        date_value.append([value[0], get_balance(value[0], value[1])])
    except Exception as e:
        print(f'Cant load value for: {value[0]} as {e}; Advised to proceed manually!')
        continue

# Delete Message Object
del message_object

# Process the Items if values exist in list else end
if len(date_value) > 0:
    # List Files Read from AWS
    aws_object = Aws_I_O()
    list_files = aws_object.get_list_files()

    # Filter 2: Get the dates not existing in AWS List
    date_value = [[x[0], x[1]] for x in date_value if x[0] not in list_files['Dates'].tolist()]
    log.info(f"Files to be created for upload/download based on data gathered from AWS bucket: {len(date_value)}")

    # Write Data in CSV Format
    path_list = write_data_csv(attachment_storage, date_value)

    # Upload Data to AWS - One file at a time
    for file in path_list:
        aws_object.upload_file(file, os.path.basename(file))
        print(f'File Uploaded Successfully: {os.path.basename(file)}')
        try:
            shutil.copy2(file, attachment_shared_storage)
            print(f'{os.path.basename(file)} Moved to Shared Storage post upload')
        except (FileNotFoundError, OSError):
            shutil.copy2(file, attachment_shared_storage_alt)
            print('Warning: File Moved to Downloads folder:\
                  Shared Storage was inaccessible')
        os.remove(file)  # Remove file from Local System post Upload
        log.info(f"Removed {file} from local disk")

    # Ger a list of Dates
    date_list = [x[0] for x in date_value]

    # Upload - Download Validation
    upload_download_match(path_list, date_value, aws_object)

    # Log Process Compeltion Time
    log.info(f"Process Completed in {round(time.time() - start_time, 1)} seconds")

    # Send Mails for each of the Dates Separately
    for dates in date_list:
        log.info(f"Sending Mail for: {str(dates)}")
        send_mail(dates, root_directory)

# Shutting Down Logs
logging.shutdown()

# Remove Log File as Process ends
os.remove(os.path.join(root_directory, f"crr_upload_{date.today().strftime('%d_%m_%Y')}.log"))
print(f'Task Completed in {round(time.time() - start_time, 1)} seconds')
