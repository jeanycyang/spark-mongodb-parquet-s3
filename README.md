# Read from MongoDB, save parquet to S3 (Runs on AWS EMR)

### args
- `[0]: date (yyyy-MM-dd)`
- `[1]: MONGODB_URI (mongodb://xxxxx,xxx)`
- `[2]: S3_BUCKET_NAME`

### How to Use
```bash
$ sbt "sparkSubmit --class Program -- yyyy-MM-dd mongodb://xxx,xxx event-log-storage"
```

### How to Package
```bash
$ sbt assembly
```
