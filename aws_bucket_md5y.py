#!/usr/bin/env python3

# AWS Transfer of Buckets


#List of needed packages
import pandas as pd
import argparse
import argcomplete
import boto3
import os
import hashlib
from datetime import date
from boto3.s3.transfer import TransferConfig


#obtain the date
def refresh_date():
    today=date.today()
    today=today.strftime("%Y%m%d")
    return today

todays_date=refresh_date()

#create md5sum func
def calculate_md5(filename):
    with open(filename, 'rb') as file:
        md5_hash = hashlib.md5()
        for chunk in iter(lambda: file.read(65536), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

#create a directory
def create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


#delete a file
def delete_file(file_path):
    try:
        os.remove(file_path)
    except FileNotFoundError:
        print(f"File {file_path} not found.")

#delete a directory
def delete_directory(directory_path):
    try:
        os.rmdir(directory_path)
    except FileNotFoundError:
        print(f"Directory {directory_path} not found.")


parser = argparse.ArgumentParser(
                    prog='aws_bucket_md5y.py',
                    description='This script will take a AWS Batch Operations manifest file and download the file from both buckets to obtain md5sum metadata.',
                    )

parser.add_argument( '-f', '--filename', help='The AWS file CSV manifest for the Batch Operations protocol.\n\
                    The structure of the AWS Batch Operations file is two columns, the first is the AWS bucket, no s3 prefix, and the second is the directory path to the file.\n\
                    Each row requires both pieces of information for one file to be transfered.',default="")
parser.add_argument( '-d', '--directory', help='A directory of AWS file CSV manifests for the Batch Operations protocol.',default="")
parser.add_argument( '-t', '--template', help="The translation template that notes the old and new bucket locations.\n\
                    A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.", required=True)
parser.add_argument( '-p', '--previous', help="The output file from this script from a previous run.\n\
                    By supplying this file, you can pick up where the previous run ended.")


argcomplete.autocomplete(parser)

args = parser.parse_args()

#pull in args as variables
file_path=args.filename
file_path=os.path.abspath(file_path)
directory_path=args.directory
directory_path=os.path.abspath(directory_path)
template_path=args.template
template_path=os.path.abspath(template_path)
previous_path=args.template
previous_path=os.path.abspath(template_path)

############
## CHEAT IN
############

file_path="to_move_all/splits/cds-255-phs002430.v1.p1-sequencefiles-p30-fy20_manifest_part2.csv"
file_path=None
template_path="cds_old_new_bucket_conversions.tsv"
previous_path="md5sum_final_files/md5sum_parts/cds-255-phs002430.v1.p1-sequencefiles-p30-fy20_manifest_part2_20230726.tsv"
directory_path='to_move_all/splits/'



#Take template and create dictionary of old : new bucket pairings.
df_temp=pd.read_csv(template_path, sep ='\t')
bucket_dict = {}

# Iterate over the rows
for index, row in df_temp.iterrows():
    # Convert the row into a dictionary entry
    bucket_dict.update({row['source_bucket']: row['destination_bucket']})

#########################
# Iterate over the files
#########################

#Determine the files to iterate over
if file_path:
    manifest_list= [os.path.basename(file_path)]
    file_dir_path= os.path.dirname(file_path)
elif directory_path:
    manifest_list=os.listdir(directory_path)
    file_dir_path= directory_path

#if there is a previous template, read it in and parse for files
if previous_path:
    df_previous=pd.read_csv(previous_path, sep='\t')
    old_files=df_previous['file_path'].values.tolist()

# Iterate
for manifest in manifest_list:
    file_path= file_dir_path + "/" + manifest

    print(file_path)

    #read in table of files
    df_bucket=pd.read_csv(file_path, header=None)

    #each file is per bucket, so obtain the first row first column to get the source bucket
    source_bucket=df_bucket.iloc[0,0]

    #based on the source bucket, get the destination bucket based on teh dictionary
    destination_bucket=bucket_dict.get(source_bucket)

    #get a list of all files within the data frame
    file_list=df_bucket[1].unique().tolist()

    #if there is a previous path provided, use it to filter out files that have been transfered.
    if previous_path:
        file_list = [x for x in file_list if x not in old_files]

    #create an output file name based on the old bucket and today's date
    output_file=os.path.splitext(os.path.basename(file_path))[0]+"_"+todays_date

    #create a directory for all files to be downloaded to
    create_directory(f"{source_bucket}_md5sum_check")

    # Create a Boto3 S3 client
    s3_client = boto3.client('s3')

    MB = 1024 * 1024

    config = TransferConfig(
        multipart_threshold = MB * 25,
        max_concurrency = 10,
        multipart_chunksize = MB * 25,
        use_threads = True
    )

    df_all=pd.DataFrame({'bucket':[],'file_path':[],'source_md5sum':[],'destination_md5sum':[],'status':[]})
    df_add=pd.DataFrame({'bucket':[],'file_path':[],'source_md5sum':[],'destination_md5sum':[],'status':[]})

    #for each file
    for file in file_list:   
        file_path=f"{source_bucket}_md5sum_check/"+os.path.basename(file)
        
        # Download the file from the source bucket and find the md5sum and output
        s3_client.download_file(source_bucket, file, file_path, Config=config)

        #find m5sum
        source_md5sum = calculate_md5(file_path)

        #delete the file in the md5sum folder
        delete_file(file_path)

        #Manipulate so that destination bucket is the base bucket, but add the initial sub-directory onto the file.
        #Create a destination object path based on the concatenation of the destination bucket and the existing directory for the file from the source bucket
        destination_components= destination_bucket.rstrip('/').split("/")
        destination_bucket_base=destination_components[0]
        destination_path="/".join(destination_components[1:])

        #for each file create a new file path, and make sure it doesn't have an accidental '/' at the beginning.
        file_new= destination_path + "/" + file
        if file_new[0]=='/':
            file_new=file_new[1:]

        #Download the file from the destination bucket and find the md5sum and output
        s3_client.download_file(destination_bucket_base, file_new, file_path, Config=config)

        #find m5sum
        destination_md5sum = calculate_md5(file_path)

        #Check to see if source and destination md5s are the same
        if source_md5sum == destination_md5sum:
            status="PASS"
        else:
            status="FAIL"

        #add row to data frame
        df_add=pd.DataFrame({'bucket':[destination_bucket],'file_path':[file],'source_md5sum':[source_md5sum],'destination_md5sum':[destination_md5sum],'status':[status]})
        df_all=pd.concat([df_all,df_add], ignore_index=True)
        df_all.to_csv(f'./{output_file}.tsv', sep="\t", index=False)

        #delete the file in the md5sum folder
        delete_file(file_path)

    ##delete the folder
    delete_directory(f"{source_bucket}_md5sum_check")
    print(f"MD5SUM CHECK COMPLETE: {source_bucket}")
