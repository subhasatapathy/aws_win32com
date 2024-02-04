# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 12:35:27 2021

@author: Subham
"""
import os
import re
import logging
import pandas as pd
from datetime import datetime
from boto3.session import Session
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class Aws_I_O:
    key_df = pd.read_csv("D:/Python/crr_upload/Keys/Liq-python-user_accessKeys.csv")
    access_key = key_df.loc[0, 'Access key ID']
    secret_key = key_df.loc[0, 'Secret access key']
    prefix = "path_storage"

    def __init__(self):
        session = Session(aws_access_key_id = self.access_key,
                          aws_secret_access_key = self.secret_key)
        s3 = session.resource('s3', verify=True)
        self.bucket = s3.Bucket('com-liquitics-customer-alm-kmb02')

    def get_list_files(self):
        """
        Get a List of files available in the S3 bucket

        Returns
        -------
        list_files : TYPE : List
            List of Files in the bucket.

        """
        # Get a List of Files available in S3 Bucket
        list_files = []
        for s3_file in self.bucket.objects.filter(Prefix = self.prefix):
            if os.path.basename(s3_file.key) != '':
                list_files.append(s3_file.key)
        # Have Dates alongside Filename
        list_dates = [datetime.strptime(re.findall("\d{1,2}\w*\d{2}", x)[0],
                                        "%d%b%y").date() for x in list_files]
        data = pd.DataFrame({"Object": list_files, "Dates": list_dates})
        log.info(f"List of files obtained from AWS bucket is of shape: {data.shape}")
        return data

    def upload_file(self, source_file_name, destination):
        """
        Upload File to AWS

        Parameters
        ----------
        source_file_name : TYPE : CSV File
            Source File from Local Disk.
        destination : TYPE : CSV File
            Destination File in Remote Directory.

        Returns
        -------
        None.

        """
        try:
            self.bucket.upload_file(source_file_name, self.prefix + destination)
            log.info(f"File Successfully uploaded to {self.prefix + destination}")
        except ClientError as e:
            logging.error(e)


    def download_file(self, source_file_name, destination):
        """
        Download File from AWS Bucket

        Parameters
        ----------
        source_file_name : TYPE : CSV File
            Source File from Remote Directory.
        destination : TYPE : CSV File
            Destination File to Local Disk.

        Returns
        -------
        None.

        """
        try:
            self.bucket.download_file(self.prefix + source_file_name, destination)
            log.info(f"File Successfully downloaded from {self.prefix + source_file_name}")
        except ClientError as e:
            log.error(f"Download Failed: {e}")
