# read from MongoDB, save parquet to S3

### env variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### args
- [0]: date (yyyy-MM-dd)
- [1]: MONGODB_URI (mongodb://xxxxx,xxx)
- [2]: S3_BUCKET_NAME

```bash
$ sbt "sparkSubmit --class Program -- yyyy-MM-dd mongodb://xxx,xxx event-log-storage"
```
