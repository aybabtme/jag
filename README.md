
# jag

Command jag audits that brigade does its work properly.

More concretely, it verifies that two S3 buckets contain the same data,
by sampling the buckets randomly at regular intervals and verifying
that the selected keys match on both buckets.

To accomplish this work, the verifier randomly selects keys in the source
bucket using a statistical model of the bucket that it builds using a
previous snapshop of the bucket's keys.








- - -
Generated by [godoc2md](http://godoc.org/github.com/davecheney/godoc2md)
