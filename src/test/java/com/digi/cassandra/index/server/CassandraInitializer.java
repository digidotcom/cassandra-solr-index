/**
 * Copyright 2017, Digi International Inc.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, you can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES 
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF 
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR 
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES 
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN 
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF 
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package com.digi.cassandra.index.server;

import static com.jayway.awaitility.Awaitility.await;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Initializes the Cassandra cluster by executing DataStreaMetaData.cql before
 * each test which clears out the TestKeyspace keyspace and recreates a fresh
 * DataStreamMetaData table.
 * 
 * The class will automatically start an embedded cassandra node (and solr
 * server) but you can set {@link #startEmbeddedCassandra} to false and adjust
 * {@link #hostIp} to point it at an existing Cassandra ring. The only
 * requirement is that the target ring have the cassandra-index jar in its lib
 * directory.
 *
 */
public class CassandraInitializer {
	/**
	 * By default an embedded Cassandra server is started and used for the
	 * tests. Setting this value to false allows for the use of an existing
	 * Cassandra ring. If set to false make sure to adjust {@link #hostIp} to
	 * point to the cluster.
	 */
	private static boolean startEmbeddedCassandra = true;

	/**
	 * Cassandra node to connect to. A single node in the cluster is fine due to
	 * autodiscovery of the CQL Driver. Solr requests will be load balanced
	 * against the discovered Cassandra nodes.  These parameters will be set automatically
	 * when using an embedded cassandra server so are only necessary when using
	 * an external ring.
	 */
	private static String hostIp = "127.0.0.1";
	private static int port = 9042;
	
	/**
	 * The embedded cassandra does not open a JMX port by default.
	 * This port will be used to set the cassandra.jmx.local.port
	 * property.
	 */
	private static int jmxPort = 7199;
	
	private static Session session;
	private static Cluster cluster;
	private static SolrServer solrServer;
	private static List<SolrServer> allSolrServers;

	/**
	 * Starts embedded Cassandra.  The keyspace is dropped and the
	 * DataStreamMetaData.cql script is run to ensure the schema
	 * is setup as expected.
	 */
	@BeforeClass
	public static void initCassandra() throws Exception {
		// start embedded cassandra once if we're set to do so
		if (startEmbeddedCassandra) {
			System.setProperty("cassandra.jmx.local.port", "" + jmxPort); 
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			hostIp = EmbeddedCassandraServerHelper.getHost();
			port = EmbeddedCassandraServerHelper.getNativeTransportPort();
		}

		// setup the cql and solr clients
		cluster = new Cluster.Builder().addContactPoints(hostIp).withPort(port).build();
		session = cluster.connect();
		solrServer = initSolrServer();
		allSolrServers = initAllSolrServers();

		// drop keyspace before each test class
		session.execute("DROP KEYSPACE IF EXISTS \"" + getKeyspace() + "\"");
		
		// and then recreate it
		load("DataStreamMetaDataTable.cql");
		load("DataStreamMetaDataIndex.cql");
	}
	
	/**
	 * Between each test the table is truncated which will cause
	 * the secondary index to clear out each Solr node.
	 */
	@Before
	public void clearCassandra() throws Exception {
		// truncate table to ensure all data is purged from solr
		session.execute(QueryBuilder.truncate(getQuotedTable()));
		
		// wait a bit for solr to commit after the truncate
		await().until(
				() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == 0L
		);
	}

	/**
	 * Shutdown the clients
	 */
	@AfterClass
	public static void cleanupCassandra() {
		try (Cluster c = cluster; Session s = session) {
			session = null;
			cluster = null;
		}

		if(solrServer != null) {
			solrServer.shutdown();
		}
		if(allSolrServers != null) {
			allSolrServers.forEach(server -> server.shutdown());
		}
	}

	/**
	 * Executes the cql statements contained in the given file against the
	 * target cluster.  Make sure each statement ends with a semicolon (;)
	 */
	protected static void load(String cqlFile) {
		ClassPathCQLDataSet dataSet = new ClassPathCQLDataSet(cqlFile, false, false);

		CQLDataLoader dataLoader = new CQLDataLoader(session);
		dataLoader.load(dataSet);
		session = dataLoader.getSession();
	}

	/**
	 * Retrieve the CQL Session
	 */
	protected static Session getSession() {
		return session;
	}
	
	/**
	 * Retrieves the CQL Cluster
	 */
	protected static Cluster getCluster() {
		return cluster;
	}

	/**
	 * Retrieve SolrServer client load balanced across
	 * all solr servers
	 */
	protected static SolrServer getSolrServer() {
		return solrServer;
	}

	/**
	 * Retrieve a list of solr clients with one corresponding
	 * to each solr server in the cluster
	 */
	protected static List<SolrServer> getAllSolrServers() {
		return allSolrServers;
	}
	
	/**
	 * Get's the unquoted keyspace used for the tests. The CQL client will
	 * already be set to this keyspace so there is no need to specify it
	 * normally.
	 */
	protected static String getKeyspace() {
		return "TestKeyspace";
	}

	/**
	 * The unquoted table name.
	 */
	protected static String getTableName() {
		return "DataStreamMetaData";
	}
	
	protected static String getQuotedKeyspace() {
		return Metadata.quote(getKeyspace());
	}
	
	protected static String getQuotedTable() {
		return Metadata.quote(getTableName());
	}

	/**
	 * The name of the Solr core based on the keyspace and table name
	 */
	private static String getSolrCoreName() {
		return getKeyspace() + "." + getTableName();
	}

	/**
	 * The full Solr URL including the core name
	 */
	private static String getSolrUrl(String host) {
		return "http://" + host + ":8983/solr/" + getSolrCoreName();
	}

	/**
	 * Builds a list of load balanced Solr Servers based on the Cassandra driver
	 * metadata intending to pick up all servers based on its node discovery.
	 */
	private static SolrServer initSolrServer() throws MalformedURLException {
		String[] solrServers = cluster.getMetadata().getAllHosts().stream()
				.map(Host::getAddress)
				.map(InetAddress::getHostAddress)
				.map(CassandraInitializer::getSolrUrl)
				.toArray(size -> new String[size]);

		return new LBHttpSolrServer(solrServers);
	}
	
	/**
	 * Returns list of SolrServers where there will be one entry per node in the cluster
	 */
	private static List<SolrServer> initAllSolrServers() throws MalformedURLException {
		return cluster.getMetadata().getAllHosts().stream()
				.map(Host::getAddress)
				.map(InetAddress::getHostAddress)
				.map(CassandraInitializer::getSolrUrl)
				.map(HttpSolrServer::new)
				.collect(Collectors.toList());
	}
}
