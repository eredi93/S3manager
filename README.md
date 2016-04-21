# S3 manager
#### manage AWS S3 buckets

This is a simple script that helps you manage S3 buckets.
With this script you can upload, download and delete files, create and delete buckets.

## Usage

S3manager git:(master)$ ./S3manager.py -h                                  
```
usage: S3 Manager [-h]
                  {download,upload,delete-file,delete-all-files,delete-bucket}
                  ...

Amazon S3 manager

positional arguments:
  {download,upload,delete-file,delete-all-files,delete-bucket}
                        sub-command help
    download            Download file from S3 bucket
    upload              Upload file to S3 bucket
    delete-file         Delete file in S3 bucket
    delete-all-files    Delete all files in S3 bucket
    delete-bucket       Delete S3 bucket

optional arguments:
  -h, --help            show this help message and exit
```