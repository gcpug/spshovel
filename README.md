# spshovel

Spanner Power shovel is a tool to output Spanner's Query result as CSV.
It helps to output huge Query results from Spanner.

## Output Query Results as CSV

### go run example

```
go run *.go -project=gcpug-public-spanner \
  -instance=merpay-sponsored-instance \
  -database=sinmetal \
  -sql-file-path=/Users/sinmetal/go/src/github.com/sinmetal/spshovel/example.sql
```

#### Do not output headers to CSV

```
go run *.go -project=gcpug-public-spanner \
  -instance=merpay-sponsored-instance \
  -database=sinmetal \
  -sql-file-path=/Users/sinmetal/go/src/github.com/sinmetal/spshovel/example.sql\
  --no-header=true
```