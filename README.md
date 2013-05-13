# Usage

This will read all the keys in a Redis database, grab each value as a Base64 encoded document, decode each into a string and bulk save to couch. It will run flush db unless --flush is set to false.

See the sister project [couchout](https://github.com/lancecarlson/couchout.go) for getting couchdb documents into Redis. 

```
couchin http://localhost:5984/db/_bulk_docs
```

Default Options:

```
  -db=0: select the Redis db integer
  -flush=false: flush Redis after finished
  -host="localhost:6379": host for Redis
  -password="": password for Redis
  -print-results=false: output the result of each bulk request
  -print-status=false: output the result the status of workers
  -save-limit=100: number of docs to save at once
  -workers=20: number of workers to batch save
```

# Install 

```
git clone git@github.com:lancecarlson/couchin.go.git
cd couchin.go
go get github.com/vmihailenco/redis # Required dependency
go build -o couchin # Builds a binary file for you. Put this in one of your PATH directories
```

# Why?

See [couchout](https://github.com/lancecarlson/couchout.go#why) readme
