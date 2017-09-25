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
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

public class SolrIndexConfiguration {
    private static Logger log = LoggerFactory.getLogger(SolrIndexConfiguration.class);

	private static final String DEFAULT_PATH = "/etc/idigi/index.conf";
	private static AtomicReference<String> PATH = new AtomicReference<>(DEFAULT_PATH);

    // The default value for WAR probably don't work very well most of the time, but
	// they're a best attempt
	private static final String DEFAULT_LIFECYCLE = "prd";
	// When we can't find the solr war and no property was specified, what path and file name
	// do we use for the Solr WAR file
	private static final String DEFAULT_UNRESOLVED_SOLR_WAR = "./solr-4.10.3.war";
	private static final String DEFAULT_SOLR_CONTEXT = "solr";
	private static final int DEFAULT_SOLR_PORT = 8983;
	
	/**
	 * maximum number of solr requests in queue
	 */
	private static final int DEFAULT_INDEX_QUEUE_MAX_SIZE = 2000;
	
	/**
	 * milliseconds to wait for request to be inserted into queue before giving up
	 */
	private static final int DEFAULT_INDEX_QUEUE_OFFER_MAX_DURATION = 5000;
	
	/**
	 * number of solr requests allowed to build up in queue before 
	 * skipping batch window 
	 */
	private static final int DEFAULT_INDEX_IMMEDIATE_BATCH_THRESHOLD = 100;
	
	/**
	 * amount of time in milliseconds that subsequent solr requests are allowed
	 * to flow into the queue before the queue is drained.
	 */
	private static final int DEFAULT_INDEX_BATCH_WINDOW_DURATION = 500;

	private static final String PROPERTY_LIFECYCLE = "dc_index.lifecycle";
	private static final String PROPERTY_SOLR_WAR = "dc_index.solr_war";
	private static final String PROPERTY_SOLR_CONTEXT = "dc_index.solr_context";
	private static final String PROPERTY_SOLR_HOME = "dc_index.solr_home";
	private static final String PROPERTY_SOLR_PORT = "dc_index.solr_port";
	private static final String PROPERTY_SOLR_ADDRESS = "dc_index.solr_address";
	
	private static final String PROPERTY_INDEX_QUEUE_MAX_SIZE = "dc_index.queue_max_size";
	private static final String PROPERTY_INDEX_QUEUE_OFFER_MAX_DURATION = "dc_index.queue_offer_max_duration";
	private static final String PROPERTY_INDEX_IMMEDIATE_BATCH_THRESHOLD = "dc_index.immediate_batch_threshold";
	private static final String PROPERTY_INDEX_BATCH_WINDOW_DURATION = "dc_index.batch_window_duration";

	private static Config defaults = null;
	static {
		// We could use a reference.conf file in classpath, but tests and other projects
		// already have one and we don't want to get mixed up
		try {
			Map<String, Object> values = new HashMap<>();
			values.put(PROPERTY_LIFECYCLE, DEFAULT_LIFECYCLE);
			values.put(PROPERTY_SOLR_CONTEXT, DEFAULT_SOLR_CONTEXT);
			// Intentionally skipping PROPERTY_SOLR_HOME don't want a default, default is "cassandra-dir/solr_data"
			values.put(PROPERTY_SOLR_PORT, DEFAULT_SOLR_PORT);
			// Intentionally skipping PROPERTY_SOLR_ADDRESS don't want a default, default is cassandra provided
			values.put(PROPERTY_INDEX_QUEUE_MAX_SIZE, DEFAULT_INDEX_QUEUE_MAX_SIZE);
			values.put(PROPERTY_INDEX_QUEUE_OFFER_MAX_DURATION, DEFAULT_INDEX_QUEUE_OFFER_MAX_DURATION);
			values.put(PROPERTY_INDEX_IMMEDIATE_BATCH_THRESHOLD, DEFAULT_INDEX_IMMEDIATE_BATCH_THRESHOLD);
			values.put(PROPERTY_INDEX_BATCH_WINDOW_DURATION, DEFAULT_INDEX_BATCH_WINDOW_DURATION);
			defaults = ConfigFactory.parseMap(values);
		}
		catch (Throwable t) {
			log.error("Failed initialization", t);
		}
	}

	/**
	 * Sets the default config file path to be used for all subsequent newly created
	 * SolrIndexConfiguration objects.
	 * If the file doesn't exist, then this method call is ignored and an error log message
	 * is written.
	 * @param f - fully qualified or relative pathname
	 */
	public static void setDefaultConfigFile(String f) {
		if (!new File(f).exists()) {
			log.error("setDefaultConfigFile(): file not found, skipping {}", f);
			return;
		}
		log.info("Set default configuration path to {}", f);
		PATH.set(f);
	}
	public static void resetDefaultConfigFile() {
		log.info("Reset default configuration path to {}", DEFAULT_PATH);
		PATH.set(DEFAULT_PATH);
	}


	private final AtomicReference<Config> config = new AtomicReference<>();

	public SolrIndexConfiguration() {
		this(PATH.get());
	}
	@VisibleForTesting
	public SolrIndexConfiguration(String path) {
		if (path != null) {
			File f = new File(path);
			if (!f.canRead()) {
				log.error("The file {} does not exist or can't be read, using a pure default configuration", path);
				config.set(defaults);
			}
			else {
				log.info("Initializing Solr index configuration using {}", f.getAbsolutePath());
				config.set(ConfigFactory.parseFile(f).withFallback(defaults));
			}
		}
		else {
			log.error("No configuration file specified, using a pure default configuration");
			config.set(defaults);
		}
		for (Map.Entry<String, ConfigValue> ent : config.get().entrySet()) {
			log.info("Configuration: {} = {}", ent.getKey(), ent.getValue().render());
		}
	}

	public String getLifecycle() {
		return config.get().getString(PROPERTY_LIFECYCLE);
	}
	public String getSolrWar() {
		if (config.get().hasPath(PROPERTY_SOLR_WAR)) {
			return config.get().getString(PROPERTY_SOLR_WAR);
		}
		// In the case of no solr war, make a best attempt at finding the correct directory
		// and the correct file in that directory.
		String file = resolveSolrWar();
		return file;
	}
	public int getSolrPort() {
		return config.get().getInt(PROPERTY_SOLR_PORT);
	}
	public String getSolrContext() {
		return config.get().getString(PROPERTY_SOLR_CONTEXT);
	}
	
	/**
	 * Maximum size of the index request queue.  Additional requests will not
	 * be accepted unless the queue size drops below this value.
	 * Attempts to add to a full queue will temporarily block until the queue is 
	 * no longer at capacity.  A request that cannot be added within a timeout
	 * will be dropped.
	 * 
	 * @return maximum size of the index request queue 
	 */
	public int getMaxQueueSize() {
		return config.get().getInt(PROPERTY_INDEX_QUEUE_MAX_SIZE);
	}
	
	/**
	 * Amount of time to wait for the queue to accept a submitted solr request 
	 * when the queue is at capacity. Value is in milliseconds.
	 * 
	 * @return number of milliseconds to wait for enqueue
	 */
	public long getQueueMaxOfferDuration() {
		return config.get().getInt(PROPERTY_INDEX_QUEUE_OFFER_MAX_DURATION);
	}
	
	/**
	 * The batching window will be avoided when we have reached this number
	 * of solr requests in the queue.
	 * 
	 * @return number of requests in the queue to trigger skipping of batch window
	 */
	public int getImmediateBatchThreshold() {
		return config.get().getInt(PROPERTY_INDEX_IMMEDIATE_BATCH_THRESHOLD);
	}
	
	/**
	 * Amount of time to wait for additional requests to flow in to the
	 * solr request queue before collecting a batch.
	 * 
	 * @return number of milliseconds to wait for batch collection
	 */
	public long getBatchWindowDuration() {
		return config.get().getInt(PROPERTY_INDEX_BATCH_WINDOW_DURATION);
	}

	private String resolveSolrWar() {
		String file = DEFAULT_UNRESOLVED_SOLR_WAR;
		// Where did we load this class from?
		// We'll get values like this:
		//   file:/Users/fkulack/src/Digi/cassandra-index/target/classes/com/digi/cassandra/index/SolrIndexer.class
		//      We will not resolve solr-war from the above path.
		//   jar:file:/Users/fkulack/.ccm/repository/2.1.13/lib/cassandra-index-1.0.0.jar!/com/digi/cassandra/index/SolrIndexConfiguration.class
		//      We will resolve solr-war from the above path from file .../2.1.13/solr-*.war
		try {
			URL classURL = this.getClass().getResource("/" + this.getClass().getName().replace('.', '/') + ".class");
			if(classURL == null) {
				log.warn("Failed to find URL for class={}", this.getClass().toString());
			}
			String url = classURL == null ? null : classURL.toString();
			log.info("Resolving solr war file from classes at {}", url);
			// If a jar file, and the jar file is in x/lib/foo.jar, then we'll look for solr-*.war in the x directory.
			if (url != null && url.startsWith("jar:file:")) {
				int ndx = url.indexOf("!");
				if (ndx > -1) {
					String jarFile = url.substring("jar:file:".length(), ndx);
					ndx = jarFile.lastIndexOf("/");
					String jarDirectory = jarFile.substring(0, ndx);
					if (jarDirectory.endsWith("/lib")) {
						String warLocation = jarDirectory.substring(0, jarDirectory.length()-4);
						log.info("Finding solr-*.war in {}", warLocation);
						Optional<Path> war = Files.list(new File(warLocation).toPath())
								.filter(f -> f.getFileName().toString().matches("solr-.*\\.war"))
								.findFirst();
						if (war.isPresent()) {
							file = war.get().toAbsolutePath().toString();
							log.info("Resolved solr war file as {}", file);
							setSolrWar(file);
						}
						else {
							log.error("Couldn't find any solr-*.war files in {}, using default {}",
							          warLocation, file);
						}
					}
				}
			}
			else {
				log.warn("Couldn't resolve the solr war file. Using default of {}", file);
			}
		}
		catch (Exception e) {
			log.error("Error trying to resolve solr-*.war file, using default {}", file, e);
		}
		return file;
	}

	private void setSolrWar(String war) {
		ConfigValue v = ConfigValueFactory.fromAnyRef(war, "Resolved at runtime");
		Config newConfig = config.get().withValue(PROPERTY_SOLR_WAR, v);
		config.set(newConfig);
	}
	/**
	 * At this point, the solr home is ALWAYS the cassandra data directory
	 * with "/solr_data" appended, and further, this method will most certainly
	 * cause the process to exit when called outside of cassandra unless
	 * the configuration or test method setSolrHome overrides it.
	 * @return
	 */
	public String getSolrHome() {
		if (config.get().hasPath(PROPERTY_SOLR_HOME)) {
			return config.get().getString(PROPERTY_SOLR_HOME);
		}
		return DatabaseDescriptor.getAllDataFileLocations()[0] + "/solr_data";
	}
	@VisibleForTesting
	public void setSolrHome(String home) {
		ConfigValue v = ConfigValueFactory.fromAnyRef(home, "Manually set");
		Config newConfig = config.get().withValue(PROPERTY_SOLR_HOME, v);
		config.set(newConfig);
	}
	/**
	 * At this point, the solr address is ALWAYS the cassandra RPC address, and further,
	 * this method will most certainly cause the process to exit when called, unless
	 * the configuration or test method setSolrAddress overrides it.
	 */
	public InetAddress getSolrAddress() {
		if (config.get().hasPath(PROPERTY_SOLR_ADDRESS)) {
			return InetAddresses.forString(config.get().getString(PROPERTY_SOLR_ADDRESS));
		}
		return DatabaseDescriptor.getRpcAddress();
	}
	@VisibleForTesting
	public void setSolrAddress(InetAddress rpcAddress) {
		ConfigValue v = ConfigValueFactory.fromAnyRef(rpcAddress.getHostAddress(), "Manually set");
		Config newConfig = config.get().withValue(PROPERTY_SOLR_ADDRESS, v);
		config.set(newConfig);
	}

	@VisibleForTesting
	public Config _getConfig() {
		return config.get();
	}
}
