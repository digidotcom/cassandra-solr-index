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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.lf5.util.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets up the default Solr configuration in preparation for Device Cloud creating indexes.
 *
 * @author fkulack
 * @author cpopp
 */
public class SolrConfigInitializer {
	private static final Logger log = LoggerFactory.getLogger(SolrConfigInitializer.class);

	public static final String DATASTREAM_V1_CONFIGSET = "datastream_v1";
	
	private final String solrHome;

	public SolrConfigInitializer(String solrHome) {
		this.solrHome = solrHome;
	}

	/**
	 * Ensures solr.xml exists in the Solr Home directory and that config sets
	 * exist for datastreams (solrconfig.xml and schema.xml)
	 */
	public void create() throws Exception {
		File solrHomeDir = new File(solrHome);
		if(!(solrHomeDir.exists() || solrHomeDir.mkdirs())) {
			throw new IllegalStateException("Failed to create directory: " + solrHomeDir);
		}
		
		try(InputStream data = getResourceOrFail("solr_v1.xml")) {
			createFile(solrHomeDir, "solr.xml", data);
		}
		
		File configSetDir = new File(solrHomeDir, "configsets");
		mkdirOrFail(configSetDir);
		
		File dataStreamV1ConfigSetDir = new File(configSetDir, DATASTREAM_V1_CONFIGSET);
		mkdirOrFail(dataStreamV1ConfigSetDir);
		
		File dataStreamV1ConfigSetConfDir = new File(dataStreamV1ConfigSetDir, "conf");
		mkdirOrFail(dataStreamV1ConfigSetConfDir);
		
		try(InputStream data = getResourceOrFail("schema_v1.xml")) {
			createFile(dataStreamV1ConfigSetConfDir, "schema.xml", data);
		}
		try(InputStream data = getResourceOrFail("solrconfig_v1.xml")) {
			createFile(dataStreamV1ConfigSetConfDir, "solrconfig.xml", data);
		}
	}
	
	/**
	 * Attempt to load the desired resource from the classpath or throw a runtime exception
	 */
	public InputStream getResourceOrFail(String resourceName) {
		InputStream data = getClass().getResourceAsStream("/" + resourceName);
		
		if(data == null) {
			throw new IllegalStateException("Packaging problem, can't find " + resourceName + " in the classpath");
		} else {
			return data;
		}
	}

	/**
	 * Create or overwrite the specified path with the given contents
	 */
	private void createFile(File parent, String fileName, InputStream contents) {
		File f = new File(parent, fileName);
		
		try(FileOutputStream fos = new FileOutputStream(f)) {
			StreamUtils.copy(contents, fos);
		} catch (IOException e) {
			throw new IllegalStateException("Failed to write " + f.getAbsolutePath(), e);
		}
	}
	
	/**
	 * Create the folder if it doesn't exist and throw
	 * and IllegalStateException if it fails to create
	 */
	private void mkdirOrFail(File directory) {
		if(!(directory.exists() || directory.mkdir())) {
			throw new IllegalStateException("Failed to create directory: " + directory);
		}
	}
}
