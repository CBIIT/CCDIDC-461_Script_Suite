#!/usr/bin/env python3

# AWS Transfer of Buckets


#List of needed packages
import pandas as pd
import argparse
import argcomplete
import boto3


parser = argparse.ArgumentParser(
                    prog='aws_bucket_copyy.py',
                    description='This script will take a AWS Batch Operations manifest file and copy the file from the source bucket to the destination bucket.\n\
                        This only works for ONE BUCKET AT A TIME.',
                    )

parser.add_argument( '-f', '--filename', help='The AWS file from the Batch Operations protocol.\n\
                    The structure of the AWS Batch Operations file is two columns, the first is the AWS bucket, no s3 prefix, and the second is the directory path to the file.\n\
                    Each row requires both pieces of information for one file to be transfered.', required=True)
parser.add_argument( '-t', '--template', help="The translation template that notes the old and new bucket locations.\n\
                    A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.", required=True)


argcomplete.autocomplete(parser)

args = parser.parse_args()

#pull in args as variables
file_path=args.filename
template_path=args.template

#Take template and create dictionary of old : new bucket pairings.
df_temp=pd.read_csv(template_path, sep ='\t')
bucket_dict = {}

# Iterate over the rows
for index, row in df_temp.iterrows():
    # Convert the row into a dictionary entry
    bucket_dict.update({row['source_bucket']: row['destination_bucket']})

df_bucket=pd.read_csv(file_path, header=None)

source_bucket=df_bucket.iloc[0,0]

destination_bucket=bucket_dict.get(source_bucket)

file_list=df_bucket[1].unique().tolist()

#Create a destination object path based on the concatenation of the destination bucket and the existing directory for the file from the source bucket
destination_components= destination_bucket.rstrip('/').split("/")
destination_bucket=destination_components[0]
destination_path="/".join(destination_components[1:])

# Create a Boto3 S3 client
s3_client = boto3.client('s3')

for file in file_list:
    # Construct the source and destination object paths
    source_object = {'Bucket': source_bucket, 'Key': file}
    
    #for each file create a new file path, and make sure it doesn't have an accidental '/' at the beginning.
    file_new= destination_path + "/" + file
    if file_new[0]=='/':
        file_new=file_new[1:]
    
    # Copy the object to the destination bucket
    s3_client.copy(CopySource=source_object, Bucket=destination_bucket, Key=file_new)
    print(f"Moved file: {file}")