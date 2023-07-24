#!/usr/bin/env python3

# Create a manifest from indexd based on the ACL given and a translation table.


#List of needed packages
import pandas as pd
import argparse
import argcomplete
import os
import requests
import time
from datetime import date

#obtain the date
def refresh_date():
    today=date.today()
    today=today.strftime("%Y%m%d")
    return today

todays_date=refresh_date()


parser = argparse.ArgumentParser(
                    prog='aws_bucket_md5y.py',
                    description='This script will take an ACL and query indexd for all files with that ACL.\n\
                    Then it will create a new manifest translated from the old bucket locations.',
                    )

parser.add_argument( '-a', '--acl', help='The ACL in indexd. Can be a list of ACLs, separated by commas.', required=True)
parser.add_argument( '-t', '--template', help="The translation template that notes the old and new bucket locations.\n\
                    A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.", required=False)

argcomplete.autocomplete(parser)

args = parser.parse_args()

#pull in args as variables
acls=args.acl
template_path=args.template

acls=acls.split(',')

indexd_url='https://nci-crdc.datacommons.io/index/index?limit=100'

#Take template and create dictionary of old : new bucket pairings.
df_temp=pd.read_csv(template_path, sep ='\t')
bucket_dict = {}

# Iterate over the rows
for index, row in df_temp.iterrows():
    # Convert the row into a dictionary entry
    bucket_dict.update({row['source_bucket']: row['destination_bucket']})


for acl in acls:
    print(acl + " starting...")
    indexd_url_acl=indexd_url+f'&acl={acl}'
    
    #initialize the dataframe
    df_all= pd.DataFrame.from_dict({'guid':[],'md5':[],'size':[],'url':[],'acl':[]})

    #create an output file name based on the old bucket and today's date
    output_file=acl+"_"+todays_date

    # Send the initial request
    time.sleep(2)
    response = requests.get(url = indexd_url_acl)
    data = response.json()

    # Process the first page of data
    for record in range(0,len(data['records'])):
        df_url = data['records'][record].get("urls", [])[0]
        df_guid = data['records'][record].get("did", [])
        df_md5 = data['records'][record].get("hashes", []).get('md5',[])
        df_size = data['records'][record].get("size", [])
        df_acl = f"['{acl}']"
        #add row to data frame
        df_add=pd.DataFrame({'guid':[df_guid],'md5':[df_md5],'size':[df_size],'url':[df_url],'acl':[df_acl]})
        df_all=pd.concat([df_all,df_add], ignore_index=True)

    df_all.to_csv(f'./{output_file}.tsv', sep="\t", index=False)
        

    # If there were more than 1000 entries on the first page
    if len(data['records']) == 100:
        # Check if there are more pages
        while len(data['records']) == 100:

            # Send the next request
            time.sleep(2)
            response = requests.get(url = indexd_url_acl+'&start='+df_all['guid'][len(df_all)-1])
            data = response.json()

            if "service failure - try again later" not in data.values():
                for record in range(0,len(data['records'])):
                    df_url = data['records'][record].get("urls", [])[0]
                    df_guid = data['records'][record].get("did", [])
                    df_md5 = data['records'][record].get("hashes", []).get('md5',[])
                    df_size = data['records'][record].get("size", [])
                    df_acl = f"['{acl}']"
                    #add row to data frame
                    df_add=pd.DataFrame({'guid':[df_guid],'md5':[df_md5],'size':[df_size],'url':[df_url],'acl':[df_acl]})
                    df_all=pd.concat([df_all,df_add], ignore_index=True)

                df_all.to_csv(f'./{output_file}.tsv', sep="\t", index=False)    

    df_all['url']=df_all['url'].replace(bucket_dict, regex=True)
    df_all.to_csv(f'./{output_file}.tsv', sep="\t", index=False) 
    print(acl+" done.")



