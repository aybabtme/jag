/*
Command jag audits that [brigade] does its work properly.

More concretely, it verifies that two S3 buckets contain the same data,
by sampling the buckets randomly at regular intervals and verifying
that the selected keys match on both buckets.

To accomplish this work, the verifier randomly selects keys in the source
bucket using a statistical model of the bucket that it builds using a
previous snapshop of the bucket's keys.

    NAME:
       jag - Audits brigade to see if it does its work properly.

    USAGE:
       jag [global options] command [command options] [arguments...]

    VERSION:
       0.1

    COMMANDS:
       makeconfig   Create a sample config file at the specified path.
       audit    Continuously samples keys in two buckets, check that they match.
       model    Computes and prints a model for the given bucket listing.
       help, h  Shows a list of commands or help for one command

    GLOBAL OPTIONS:
       --version, -v    print the version
       --help, -h       show help


[brigade]: https://github.com/Shopify/brigade
*/
package main
