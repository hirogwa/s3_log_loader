# s3_log_loader
Loads and parses S3 logs and inserts them as is into SQL database. The S3 objects are deleted after being inserted.

## Prerequisites
* Set up your database
* Create your own settings.py

## Run
```
$ python s3logs.py --dry-run
```

## Reference
* http://docs.aws.amazon.com/AmazonS3/latest/dev/ServerLogs.html
* http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
