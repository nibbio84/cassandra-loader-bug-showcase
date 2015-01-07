# cassandra-loader-bug-showcase
This project has been created to demonstrate an issue with async writes in Cassandra 2.1.2 and its Java driver (2.1.3). Data is lost (or written with a consistency level lower than *ALL*) but the writing application receives no errors.

The issue has been posted on the following places for a solution:

* http://stackoverflow.com/questions/27667228/async-writes-are-broken-in-cassandra
* https://datastax-oss.atlassian.net/browse/JAVA-604

No solution has been found yet.

## Running the application
TO BE COMPLETED

## Checking the results
TO BE COMPLETED