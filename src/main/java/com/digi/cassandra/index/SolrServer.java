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
package com.digi.cassandra.index;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.handler.component.CassandraHttpShardHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class SolrServer {
	private static final Logger log = LoggerFactory.getLogger(SolrServer.class);

	private static volatile boolean started = false;
	private static AtomicReference<SolrIndexConfiguration> configuration = new AtomicReference<>();

	public static void start() {
		start(null);
	}

	/**
	 * Config file is only passed in for testing purposes. Production code defers configuration
	 * entirely to the SolrIndexConfiguration() class default files
	 */
	private static void start(File configFile) {
		synchronized (SolrServer.class) {
			if (started) {
				return;
			}
			try {
				SolrIndexConfiguration config = configFile == null
						? new SolrIndexConfiguration() : new SolrIndexConfiguration(configFile.getAbsolutePath());
				configuration.set(config);
				// Need to initialize the Solr home direcory with a minimal set
				// of directories and config files (solr.xml) before starting Solr
				SolrConfigInitializer solrConfig = new SolrConfigInitializer(config.getSolrHome());
				solrConfig.create();

				System.setProperty("solr.solr.home", config.getSolrHome());

				// Should set based on rpc address (or listen address, but
				// then we'd have to make Jetty listen on both) of Cassandra
				System.setProperty("host", config.getSolrAddress().getHostAddress());
				InetSocketAddress solrAddress = new InetSocketAddress(config.getSolrAddress(), config.getSolrPort());
				Server server = new Server(solrAddress);

				WebAppContext webapp = new WebAppContext();
				webapp.setContextPath("/" + config.getSolrContext());
				File war = new File(config.getSolrWar());
				if (!war.exists()) {
					log.error("Packaging or configuration error, Solr WAR file {} does not exist", war.getAbsolutePath());
				}
				webapp.setWar(config.getSolrWar());

				// The CassandraHttpShardHandlerFactory is in the parent class
				// and subclasses ShardHandlerFactory which would be in the
				// child classloader without the following (resulting in a class cast exception)
				webapp.setParentLoaderPriority(true);
				
				server.setHandler(webapp);

				log.info("Starting Solr " + solrAddress);
				server.start();

				started = true;
			} catch (Throwable e) {
				// Do not throw exception, that could cause Cassandra to fail to start which is much
				// worse than just failing to start Solr.
				log.error("Failed to start Solr Server", e);
				e.printStackTrace();
			}
		}
	}

	protected static boolean isStarted() {
		synchronized(SolrServer.class) {
			return started;
		}
	}

	/**
	 * After a successful startup, the configuration is initialized.
	 * @return null if not started
	 */
	public static SolrIndexConfiguration config() {
		return configuration.get();
	}

	public static void main(String[] args) throws Exception {
		File f = File.createTempFile("SolrServerStandAlone-", ".conf");
		f.deleteOnExit();

		// Create a config file to simulate /etc/idigi/index.conf, using only the
		// values which shouldn't be default for a stand alone test.

		// Must set a minimum of solr_home and solr_address because otherwise we'll try
		// to get those from cassandra config and we're not running cassandra.
		PrintWriter pw = new PrintWriter(f, "UTF-8");
		pw.println("dc_index {");
		// pw.println("  lifecycle = prd");
		pw.println("  solr_war = ./solr-4.10.3.war"); // From workspace/cwd location in eclipse
		// pw.println("  solr_war = /Users/cpopp/git/cassandra-index/solr-4.10.3.war");
		// pw.println("  solr_context = solr");
		pw.println("  solr_port = 8983");  // Default solr can run at the same time
		pw.println("  solr_home = ./solr_data");
		pw.println("  solr_address = 127.0.0.1");
		pw.println("}");
		pw.close();

		// we also need to override the shard listing since we won't be able
		// to get the information from Cassandra if we're not running within
		// its JVM.
		List<String> solrNodes = ImmutableList.of("127.0.0.1", "localhost");
		CassandraHttpShardHandler.overrideAvailableSolrNodes(solrNodes);
		
		SolrServer.start(f);
	}
}
