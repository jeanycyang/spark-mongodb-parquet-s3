# read from MongoDB, save parquet to S3

### env variables needed:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_BUCKET_NAME`
- `MONGODB_URI` (||= local mongodb)

### args
- [0]: date (yyyy-MM-dd)

```bash
$ sbt "sparkSubmit --class Program -- yyyy-MM-dd"
```
