# Cassandra Custom Solr Index
This project represents a custom secondary index that plugs into Cassandra which will start an embedded Solr Server and index data so it is queryable.  This has primarily been tested against Cassandra 2.1.13 with no known issues in an actively used cluster.  It is intended as an example and may not work for your specific use case and requirements.

## Overview
The custom secondary index will start an embedded Solr server within the Cassandra JVM whenever at least one table specifies the index on a column.  The index will automatically prime the Solr home directory with the required `solr.xml`, `solrconfig.xml`, and `schema.xml`.  To tweak these see `solr_v1.xml`, `schema_v1.xml`, and `solrconfig_v1.xml` in the project.

### Solr Cores
The index will automatically create a core in Solr for each keyspace and table specifying an index.  If you have a table TestKeyspace with table named DataStreamMetaData then a core named TestKeyspace.DataStreamMetaData will exist in Solr.  Multiple keyspaces and tables can co-exist and each will get their own core.

### Defining the Index
The index should be defined on one column in the table.  It will index every column in the table that is defined in `schema.xml`.

Each row in the table will be treated as a separate document in Solr with columns corresponding to fields in the document.  If a row is inserted, updated, or deleted the index will automatically update Solr with the new information.

### Batching
To increase indexing performance updates to Cassandra are batched to Solr.  You can tweak the batching, but by default updates are placed in a queue for asynchronous processing.  A thread will periodically grab batches (with at most a 500ms delay by default) and send them to Solr.  To avoid memory issues the queue has a maximum size and will put some backpressure on Cassandra by blocking writes if the queue maximum is reached.  The blocking time is configurable such that index requests can be discarded while still allowing th write to Cassandra to succeed.

The batch queue is not persisted anywhere so there is a slight possibility of data not being indexed if your node goes down in such a way that the batch queue is not able to be drained before the node is shutdown.  The data will still be in the table so you can ensure the data is in Solr by performing a rebuild of the index after bringing the node back up.

### Consistency
Updates written to Solr are not immediately visible for querying by default and there is no option to force the indexer to commit after writing the data.  You can tweak autoCommit settings in `solrconfig.xml`.  The example `solrconfig.xml` has a soft commit happen at least once a second.  With the batching delay default of 500ms and the 1000ms soft autoCommit time typically you'll see documents show up in queries within about 1500ms unless indexing is under a heavy load delaying batch processing.

With the asynchronous batching data may not have been written to Solr by the time the Cassandra query completes.  This means that forcing a commit in Solr after your Cassandra query returns may not result in the documents being visible for a query.

### Index Functionality
Standard functionality expected of a custom secondary index exists.  Existing data will be indexed when the index is first specified on the table, nodetool `rebuild_index` will cause the index to be updated with the current data in the table, and a `truncate` of the table will clear out documents from the index.

### How it Works
Each Cassandra node gets its own embedded Solr server and they are completely independent when updating the index.  Select queries however are automatically distributed.  You can send a select query to any Solr node and it will be distributed to enough Solr servers to ensure the entire token range is covered.  You can think of the reads as being `LOCAL_ONE`.  Queries will touch the entire token range so they will fail if all replicas are down for any token range.

### Example Scenario
Consider an eight node ring with a replication factor of three.  With the index specified Solr will be running on each node.  If a new row is inserted, it will be replicated to three Cassandra nodes.  The index running on those three nodes will independently update their local Solr index.  Once written the data can be queried.  

When a Solr query is made, the request will be distributed to at least one replica for a token range (for eight nodes with rf=three the request will be forwarded to at least three nodes).  This distribution ensures that at least one node having indexed the previous write will process the query so the new row is returned.  Given the LOCAL_ONE style consistency for the Solr queries, if there is data loss on one of the three replicas it is possible for the new row to not be not be returned in a Solr query.

## Getting Started with CCM
This section includes steps to get the embedded Solr serving running on two nodes via CCM.

1. Clone this repository

2. Create Cassandra 2.1.13 nodes with CCM:
	 1. Instructions assume two local interfaces 127.0.0.1 and 127.0.0.2 are available
	 2. `ccm create cas-2-1-13 -v 2.1.13 -n 2`

3. Run `mvn package` in cassandra-index project.  This will run unit tests that spin up embedded Cassandra/Solr on standard ports...if you want to skip this and just build the artifacts use `mvn package -Dmaven.test.skip=true`.

4. Put solr-4.10.3.war in the Cassandra directory `cp solr-4.10.3.war ~/.ccm/repository/2.1.13/` (solr-4.10.3.war is downloaded as part of packaging)

5. Put cassandra-index-1.0.0.jar in Cassandra lib directory `cp target/cassandra-index-1.0.0.jar ~/.ccm/repository/2.1.13/lib/`

6. Start ccm `ccm start`

7. Ensure your keyspace and table exist with the desired index.  The index will pulled desired columns from the Solr schema.xml so you should only specify it on one column in your table.

	```
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
	);
	
	CREATE CUSTOM INDEX "TestKeyspace_DataStreamMetaData_rowKeySearch_index" ON "TestKeyspace"."DataStreamMetaData" ("rowKeySearch") USING 'com.digi.cassandra.index.SolrIndexer';
	````

8. Creating the index should start Solr on each node.  Solr should now be available for each rpc_address associated with the nodes in your cluster, e.g. [http://127.0.0.1:8983/solr]()

9. The index will automatically create the core in Solr.  Data inserted into the table should now be available via Solr.

## Deploying
Deploying the index to Cassandra requires placing `cassandra-index-1.0.0.jar` in Cassandra lib directory and `solr-4.10.3.war` in the Cassandra home directory.  If you use debians `mvn package` will generate one that will handle placement of the jar and war file (see `cassandra-index_1.0.0-$timestamp.deb`) 

### Logging
The default Cassandra logging configuration will result in Solr logging quite a bit.  The following will help cut down on excessive log messages (add to `/etc/cassandra/logback.xml` or wherever your logging configuration is at).

    <logger name="org.apache.solr.core.SolrCore" level="WARN"/>
    <logger name="org.apache.solr.core.RequestHandlers" level="WARN"/>
    <logger name="org.apache.solr.core.QuerySenderListener" level="WARN"/>
    <logger name="org.apache.solr.core.SolrDeletionPolicy.java" level="WARN"/>
    <logger name="org.apache.solr.update" level="WARN"/>
    <logger name="org.apache.solr.search.SolrIndexSearcher" level="WARN"/>
    <logger name="org.apache.solr.handler.component" level="WARN"/>

## Limitations and Concerns
- Since requests to Solr are batched asynchronously for performance there is a chance that writes to Solr will be lost.  The Cassandra JVM would have to die while a Solr request is queued but not in the Cassandra commit log.  The queue size is quite small by default so it would be pretty unexpected for a request to stay queued long enough for it to no longer be in the commit log.  A rebuild of the index would ensure that data is indexed after an event like this.
- Asynchronous batching of requests also means that data may not be in Solr by the time the Cassandra write completes.
- Schema types that map to Strings are truncated to a length of 5000 in the indexer.  This is primarily to avoid data exceeding Solr's max length of 32666 from causing an entire batch of documents from being aborted.  For our use case even Strings of 5000 are considered excessive so we limited it to that...see SolrIndexer.index() to tweak this behavior.  You could choose to allow strings of any length through and handle it in your Solr analyzers.

## License

This software is open-source software. Copyright Digi International, 2017.

This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES 
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF 
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR 
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES 
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN 
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF 
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

### Licensing terms for Apache Solr

Licensed under the Apache License, Version 2.0

See https://www.apache.org/licenses/LICENSE-2.0

### Licensing terms for Maven provided dependencies

Libraries defined in the Maven pom.xml may have different licensing terms
