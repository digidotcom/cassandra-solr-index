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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used as the target of the -javaagent parameter for a Cassandra
 * server that should also run a solr workload.
 * Some details:
 * - A premain() method that is invoked prior to the CassandraDaemon main() method
 * - The javaagent parameter specifies the jar that this class is part of
 * - The jar's manifest defines a Premain-Class entry with this class as a target.
 *
 * See http://docs.oracle.com/javase/8/docs/api/java/lang/instrument/package-summary.html, there
 * are no modeling restrictions on what the agent premain method may do.
 * Anything application main can do, including creating threads, is legal from premain.
 *
 * @author fkulack
 */
public class SolrStartingAgent implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(SolrStartingAgent.class);

	/**
	 * The main entry point. Start a thread that watches the cassandra initialization
	 * and starts the solr server as appropriate if it was not otherwise started.
	 *
	 * @param args
	 */
	public static void premain(String args) {
        log.info("Starting SolrStartingAgent.premain(): for cluster " + DatabaseDescriptor.getClusterName() + ", arguments=" + args);
        if (args != null && args.isEmpty() == false) {
            String defaultFileName = null;
        	args = args.trim();
        	if (!args.isEmpty()) {
        		String pValues[] = args.split(",");
        		for (int i=0; i<pValues.length; ++i) {
        			String pv[] = pValues[i].trim().split("=");
        			if (pv[0].equalsIgnoreCase("config")) {
        				defaultFileName = pv[1].trim();
        			}
        		}
        	}
        	if (defaultFileName != null) {
        		SolrIndexConfiguration.setDefaultConfigFile(defaultFileName);
        	}
        }
        Thread t = new Thread(new SolrStartingAgent());
        t.setName("Solr Server Starter");
        t.setDaemon(true);
        t.start();
    }

	static class Server {
		final InetAddress addr;
		final int port;
		public Server(InetAddress addr, int port) {
			this.addr = addr;
			this.port = port;
		}

		public boolean isAvailable() {
			try (Socket cql = new Socket()) {
				cql.connect(new InetSocketAddress(addr, port), 5);
				return cql.isConnected();
			}
			catch (IOException e) {
				return false;
			}
		}
	}

	/**
	 * We wait for the native port (CQL) to be available on the locally configured
	 * cassandra, because if we skip that, we get screwed up logs and occasionally a hang
	 * and other weird behavior.
	 * The transports for CQL and Thrift are started at the end of the CassandraDaemon startup,
	 * and, if we haven't yet started Solr, we will at that time.
	 * Note that in the case of an existing Solr index in a schema, our SolrServer will
	 * have been started already by Cassandra startup of the ColumnFamily holding the indexes.
	 * That's no problem, we'll just go away.
	 */
	@Override
	public void run() {
		try {
			Server server = new Server(DatabaseDescriptor.getRpcAddress(), DatabaseDescriptor.getNativeTransportPort());
			while (!SolrServer.isStarted() && !server.isAvailable()) {
				log.info("Wait for {} at {} on port {}", DatabaseDescriptor.getClusterName(),
				         DatabaseDescriptor.getRpcAddress(), DatabaseDescriptor.getNativeTransportPort());
				TimeUnit.SECONDS.sleep(5);
			}
			// The only purpose for this check is to avoid spurious log messages when searching/debugging
			if (!SolrServer.isStarted()) {
				log.info("Starting solr server...");
				SolrServer.start();
			}
			else {
				log.info("Solr server was confirmed started");
				// We could confirm this on a deeper level, by opening the port and doing
				// some queries. But what would we do in that case? We'll defer until that time
				// we build requirements for health detection and recovery.
			}
		}
		catch (Exception e) {
			log.error("Fatal error watching for CQL access to " + DatabaseDescriptor.getClusterName() +
			          " at " + DatabaseDescriptor.getRpcAddress() +
			          " on port " + DatabaseDescriptor.getNativeTransportPort() +
			          ", failed to start Solr Server", e);
			e.printStackTrace();
		}
	}

}
