# Usage

This will read all the keys in a Redis database, grab each value as a Base64 encoded document, decode each into a string and bulk save to couch. It will run flush db unless --flush is set to false.

See the sister project [couchout](https://github.com/lancecarlson/couchout.go) for getting couchdb documents into Redis. 

```
couchin --save-url http://localhost:5984/db/_bulk_docs
```

# Install 

```
git clone git@github.com:lancecarlson/couchin.go.git
cd couchin.go
go get github.com/vmihailenco/redis # Required dependency
go build -o couchin # Builds a binary file for you. Put this in one of your PATH directories
```
