# Cassandra Issue #604 Showcase
This project has been created to demonstrate an issue with async writes in Cassandra 2.1.2 and its Java driver (2.1.3). Data is lost (or written with a consistency level lower than *ALL*) but the writing application receives no errors.

The issue has been posted on the following places for a solution:

* https://datastax-oss.atlassian.net/browse/JAVA-604
* http://stackoverflow.com/questions/27667228/async-writes-are-broken-in-cassandra

**No solution has been found**, yet.

## Environment
The environment used for the tests is composed of 12 Amazon EC2 instances (r3.2xlarge), each one with 3 EBS (IOP) disks. Cassandra is configured in JBOD mode on all 3 disks.

Configuration files:

* The database schema can be found [here](config/schema.cql)
* The *cassandra.yaml* configuration file can be found [here](config/cassandra.yaml)

The jar should be put on the home directory of *ec2-user*. It can be built using the command:

    mvn package

## Writing the rows
The issue (data loss) is present when running the application with the following startup script. The script assume that 9 out of 12 cassandra node (named slave1, …, slave9) can be accessed from the startup machine with ssh authentication keys. The other 3 nodes are called *cassandra1-2-3* and are of the same type but are not used for running the script (they are part of the same rack/datacenter).

Note that the write consistency level is set to **ALL**.

    ssh ec2-user@slave1 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave1 ALL Async 1 10 -1 load_table 0 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave2 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave2 ALL Async 1 10 -1 load_table 1000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave3 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave3 ALL Async 1 10 -1 load_table 2000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave4 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave4 ALL Async 1 10 -1 load_table 3000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave5 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave5 ALL Async 1 10 -1 load_table 4000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave6 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave6 ALL Async 1 10 -1 load_table 5000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave7 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave7 ALL Async 1 10 -1 load_table 6000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave8 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave8 ALL Async 1 10 -1 load_table 7000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    ssh ec2-user@slave9 -q -f "nohup java -jar /home/ec2-user/cassandra-loader.jar slave9 ALL Async 1 10 -1 load_table 8000000 1000000 20150101 4 > /home/ec2-user/cassandra-loader_async.log 2>&1 &"

    echo "Started Async batch on 9 slaves"

The parameters passed to the application are (in order):

* cassandra_host (the contact host of the cassandra cluster)
* consistency_level (ONE, QUORUM, ALL ...)
* loader_type (the type of loader in use. Possible values: [Sync|Async|AsyncBatch|SyncBatch])
* threads (the number 'n' of threads to use. Each thread gets about 'point_count/n' rows to write)
* max_concurrent_writes_per_thread (the number of concurrent writes that each thread will execute in Async|AsyncBatch mode)
* max_statements_in_batch (the maximum number of write operation present in each batch, in SyncBatch|AsyncBatch mode)
* output_table (the output cassandra table. The keyspace is always 'eng') 
* point_start (the numeric identifier of the first point to use, each one identifies a partition key of type LOAD00..00XXX having 24 total characters)
* point_count (the number of points to write)
* day (the reference day in yyyyMMdd format)
* clustering_day (an integer field used to group rows, you can use always 0)

## Counting the rows
The file *cassandra-loader_async.log* written by the above scripts (in all 9 machines) will contain the string “END” at the end of the process. Each log file will indicate the number of errors thrown by the threads (1 thread per machine in the above example).

After checking that all 9 instances have terminated with number of errors reported equals to *0*, the number of rows actually written can be checked with a spark-cassandra-connector script.

The test assumes that no issues are present in this phase. This can be assumed as the total count is correct when the loader script is run in the *Sync* instead of the *Async* mode.

    val count = sc.cassandraTable[(String, String, String)]("eng", "load_table").select("point_id", "load_type", "date").where("partition_day=? and date=?", 4, "20140101").count

    println(count)

The consistency level of this phase is set to *ONE* using the following parameter on the spark-submit application (set with *\-\-conf*):

    spark.cassandra.input.consistency.level=ONE
    
## Results
The loading script should write 9 millions rows (when no errors are thrown by none of the 9 machines).

The counting script returns 9 millions rows only in the *Sync* mode.

The *Async* mode returns few thousand rows less than 9 millions. **Every count returns a different number**.