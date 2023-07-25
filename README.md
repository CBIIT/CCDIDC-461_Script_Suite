# CCDIDC-461_Script_Suite
A set of scripts that were used in completing the task of CCDIDC-461.

## Steps to Correct Buckets
### Step 1: Create the config files
The AWS manifests were first set up, as a two column CSV with no header that contained a bucket location column and the key column, which was the directory path and file location. For example:

|||
|-----|-----|
|bucket_1|path/to/file.txt|
|bucket_1|file2.txt|
|bucket_1|different/path/file3.tsv|

Then a translation TSV was created that had the pairwise linking of the `source_bucket` and `destination_bucket` location. For example:

|source_bucket|destination_bucket|
|-----|-----|
|bucket_1|bucket_a|
|bucket_2|bucket_b|
|bucket_3|bucket_c|

### Step 2: Run aws_bucket_copyy.py
This step will use the AWS manifest and the translation TSV file to inform the script as to what files should be copied and to where the new bucket location:

```
python aws_bucket_copyy.py -h

usage: aws_bucket_copyy.py [-h] -f FILENAME -t TEMPLATE

This script will take a AWS Batch Operations manifest file and copy the file from the source bucket to the destination bucket.

optional arguments:
  -h, --help            show this help message and exit
  -f FILENAME, --filename FILENAME
                        The AWS file from the Batch Operations protocol. The structure of the AWS Batch Operations file is two columns, the first is the AWS bucket, no s3
                        prefix, and the second is the directory path to the file. Each row requires both pieces of information for one file to be transfered.
  -t TEMPLATE, --template TEMPLATE
                        The translation template that notes the old and new bucket locations. A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.
```

After the files have been copied, we need to check the file integrity for both the new and old file to ensure a complete copy without complications.

### Step 3: Run aws_bucket_md5y.py
This step will use the AWS manifest, or a directory of AWS manifests, and the translation TSV to download the file from both the source and destination bucket, run the md5sum on both files and then compare them to each other. It will then give a `PASS/FAIL` based on the md5sum value outcome.

```
python aws_bucket_md5y.py -h

usage: aws_bucket_md5y.py [-h] [-f FILENAME] [-d DIRECTORY] -t TEMPLATE

This script will take a AWS Batch Operations manifest file and download the file from both buckets to obtain md5sum metadata.

optional arguments:
  -h, --help            show this help message and exit
  -f FILENAME, --filename FILENAME
                        The AWS file CSV manifest for the Batch Operations protocol. The structure of the AWS Batch Operations file is two columns, the first is the AWS
                        bucket, no s3 prefix, and the second is the directory path to the file. Each row requires both pieces of information for one file to be
                        transfered.
  -d DIRECTORY, --directory DIRECTORY
                        A directory of AWS file CSV manifests for the Batch Operations protocol.
  -t TEMPLATE, --template TEMPLATE
                        The translation template that notes the old and new bucket locations. A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.
```

This process can take a while, so it is best to assume that most if not all files did correctly transfer. If there are issues, they can be resolved by transfering again using the previous copy script. The next step does not require that the file be complete to move forward.

### Step 4: Updating indexd records
The records of indexd need to be updated from the previous records with the now incorrect file bucket paths. A new manifest will need to be created for re-indexing of these entries. The following script will take ACL values, if there are a list supply the list with commas, and the translation TSV to create a new indexing manifest:

```
python indexd_manifest_buildery.py -h

usage: aws_bucket_md5y.py [-h] -a ACL [-t TEMPLATE]

This script will take an ACL and query indexd for all files with that ACL. Then it will create a new manifest translated from the old bucket locations.

optional arguments:
  -h, --help            show this help message and exit
  -a ACL, --acl ACL     The ACL in indexd. Can be a list of ACLs, separated by commas.
  -t TEMPLATE, --template TEMPLATE
                        The translation template that notes the old and new bucket locations. A TSV file that has two columns, 'source_bucket' and 'destination_bucket'.
```

With the new manifest sent to DCF for re-indexing, the GUIDs that are applied to the files will be updated and all calls to the file GUIDs will now apply to the new location.
