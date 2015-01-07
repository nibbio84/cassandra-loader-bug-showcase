CREATE KEYSPACE eng WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;


CREATE TABLE eng.load_table (
    point_id text,
    month text,
    clustering_day int,
    date text,
    load_type text,
    load_value list<int>,
    quality_word_value list<boolean>,
    sampling_interval int,
    total_consumption bigint,
    validation_codes list<text>,
    validation_status text,
    PRIMARY KEY ((point_id, month), clustering_day, date, load_type)
) WITH CLUSTERING ORDER BY (clustering_day ASC, date ASC, load_type ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
	AND compaction = { 'class' :  'LeveledCompactionStrategy'  }
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


