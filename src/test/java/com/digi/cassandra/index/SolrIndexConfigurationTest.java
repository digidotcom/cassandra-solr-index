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
import java.net.Inet4Address;

import org.junit.Assert;
import org.junit.Test;

public class SolrIndexConfigurationTest {

	@Test
	public void testDefaultConfiguration() throws Exception {
		SolrIndexConfiguration config = new SolrIndexConfiguration(null);
		config.setSolrAddress(Inet4Address.getByName("127.0.0.1"));
		config.setSolrHome("./solr_data");

		Assert.assertEquals("prd", config.getLifecycle());

		Assert.assertEquals(8983, config.getSolrPort());
		Assert.assertEquals("solr", config.getSolrContext());
		Assert.assertEquals("./solr_data", config.getSolrHome());
		Assert.assertEquals("127.0.0.1", config.getSolrAddress().getHostAddress());
		Assert.assertTrue(config.getSolrWar(), config.getSolrWar().contains("/solr-"));
		Assert.assertTrue(config.getSolrWar(), config.getSolrWar().endsWith(".war"));
		
		// Test queuing/batching defaults
		Assert.assertEquals(2000, config.getMaxQueueSize());
		Assert.assertEquals(5000, config.getQueueMaxOfferDuration());
		Assert.assertEquals(100, config.getImmediateBatchThreshold());
		Assert.assertEquals(500, config.getBatchWindowDuration());
	}

	@Test
	public void testFileBasedConfig() throws Exception {
		File f = File.createTempFile("SolrIndexConfigurationTest-", ".conf");
		f.deleteOnExit();
		String war = "/a/b/blah.war";
		String context = "ROOT";
		String lifecycle = "dev";
		String zk = "127.0.0.2:2222";
		String home = "/c/d/solr_data";
		int port = 8910;
		String addr = "127.0.0.42";
		int maxQueueSize = 555;
		long maxOfferDuration = 444;
		int immediateBatchThreshold = 333;
		long batchWindowDuration = 222;
		

		PrintWriter pw = new PrintWriter(f);
		pw.println("dc_index {");
		pw.println(" lifecycle = " + lifecycle);
		pw.println("  zk_servers = \"" + zk + "\"");
		pw.println("  solr_war = " + war);
		pw.println("  solr_context = " + context);
		pw.println("  solr_home = " + home);
		pw.println("  solr_port = " + port);
		pw.println("  solr_address = " + addr);
		pw.println("  queue_max_size = " + maxQueueSize);
		pw.println("  queue_offer_max_duration = " + maxOfferDuration);
		pw.println("  immediate_batch_threshold = " + immediateBatchThreshold);
		pw.println("  batch_window_duration = " + batchWindowDuration);
		pw.println("}");
		pw.close();

		SolrIndexConfiguration config = new SolrIndexConfiguration(f.getAbsolutePath());

		Assert.assertEquals(lifecycle, config.getLifecycle());

		Assert.assertEquals(port, config.getSolrPort());
		Assert.assertEquals(context, config.getSolrContext());
		Assert.assertEquals(home, config.getSolrHome());
		Assert.assertEquals("/" + addr, config.getSolrAddress().toString());
		Assert.assertEquals(war, config.getSolrWar());
		
		Assert.assertEquals(maxQueueSize, config.getMaxQueueSize());
		Assert.assertEquals(maxOfferDuration, config.getQueueMaxOfferDuration());
		Assert.assertEquals(immediateBatchThreshold, config.getImmediateBatchThreshold());
		Assert.assertEquals(batchWindowDuration, config.getBatchWindowDuration());
	}

	@Test
	public void testSomeDefaultsAndFileBasedConfig() throws Exception {
		File f = File.createTempFile("SolrIndexConfigurationTest-", ".conf");
		f.deleteOnExit();
		String war = "/a/b/blah.war";
		String context = "ROOT";
		String home = "/c/d/solr_data";
		String addr = "127.0.0.42";
		int maxQueueSize = 17;
		long queueMaxOfferDuration = 27;

		PrintWriter pw = new PrintWriter(f);
		pw.println("dc_index {");
		pw.println("  solr_war = " + war);
		pw.println("  solr_context = " + context);
		pw.println("  solr_home = " + home);
		pw.println("  solr_address = " + addr);
		pw.println("  queue_max_size = " + maxQueueSize);
		pw.println("  queue_offer_max_duration = " + queueMaxOfferDuration);
		pw.println("}");
		pw.close();

		SolrIndexConfiguration config = new SolrIndexConfiguration(f.getAbsolutePath());

		Assert.assertEquals("prd", config.getLifecycle());

		Assert.assertEquals(8983, config.getSolrPort());
		Assert.assertEquals(context, config.getSolrContext());
		Assert.assertEquals(home, config.getSolrHome());
		Assert.assertEquals(addr, config.getSolrAddress().getHostAddress());
		Assert.assertEquals(war, config.getSolrWar());
		Assert.assertEquals(maxQueueSize, config.getMaxQueueSize());
		Assert.assertEquals(queueMaxOfferDuration, config.getQueueMaxOfferDuration());
		Assert.assertEquals(100, config.getImmediateBatchThreshold());
		Assert.assertEquals(500, config.getBatchWindowDuration());
	}

	@Test
	public void testDefaultOverrideFileBasedConfig() throws Exception {
		File f = File.createTempFile("SolrIndexConfigurationTest-", ".conf");
		f.deleteOnExit();
		String war = "/a/b/blah.war";
		String context = "ROOT";
		String home = "/c/d/solr_data";
		String addr = "127.0.0.42";
		int maxQueueSize = 555;
		long maxOfferDuration = 444;
		int immediateBatchThreshold = 333;
		long batchWindowDuration = 222;

		PrintWriter pw = new PrintWriter(f);
		pw.println("dc_index {");
		pw.println("  solr_war = " + war);
		pw.println("  solr_context = " + context);
		pw.println("  solr_home = " + home);
		pw.println("  solr_address = " + addr);
		pw.println("  queue_max_size = " + maxQueueSize);
		pw.println("  queue_offer_max_duration = " + maxOfferDuration);
		pw.println("  immediate_batch_threshold = " + immediateBatchThreshold);
		pw.println("  batch_window_duration = " + batchWindowDuration);
		pw.println("}");
		pw.close();

		SolrIndexConfiguration.setDefaultConfigFile(f.getAbsolutePath());
		try {
			SolrIndexConfiguration config = new SolrIndexConfiguration();

			Assert.assertEquals("prd", config.getLifecycle());

			Assert.assertEquals(8983, config.getSolrPort());
			Assert.assertEquals(context, config.getSolrContext());
			Assert.assertEquals(home, config.getSolrHome());
			Assert.assertEquals(addr, config.getSolrAddress().getHostAddress());
			Assert.assertEquals(war, config.getSolrWar());
			Assert.assertEquals(maxQueueSize, config.getMaxQueueSize());
			Assert.assertEquals(maxOfferDuration, config.getQueueMaxOfferDuration());
			Assert.assertEquals(immediateBatchThreshold, config.getImmediateBatchThreshold());
			Assert.assertEquals(batchWindowDuration, config.getBatchWindowDuration());

		}
		finally {
			SolrIndexConfiguration.resetDefaultConfigFile();
		}
	}
}
