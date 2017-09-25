CREATE KEYSPACE "TestKeyspace" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE "TestKeyspace";

CREATE TABLE "DataStreamMetaData" (
    key text PRIMARY KEY,
    "_docBoost" text,
    "cstId" int,
    "currentValue" blob,
    "dataType" text,
    description text,
    "forwardTo" text,
    "lastUpdated" bigint,
    "rowKeySearch" text,
    solr_query text,
    "streamId" text,
    units text
) WITH COMPACT STORAGE
    AND bloom_filter_fp_chance = 0.1
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'sstable_size_in_mb': '160', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = 'NONE';