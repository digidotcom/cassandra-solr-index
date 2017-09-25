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

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple index that starts Solr the first time init()
 * is called but otherwise just logs.
 */
public class SolrStartingIndex extends PerRowSecondaryIndex {
	private final static String INDEX_NAME = "SolrStartingIndexer";

	protected static final Logger logger = LoggerFactory.getLogger(SolrStartingIndex.class);

	@Override
	public void index(ByteBuffer rowKey, ColumnFamily cf) {
		logger.info("index, columnFamily={}", cf);

	}

	@Override
	public void delete(DecoratedKey key, Group opGroup) {
		logger.info("delete, key={}, opGroup={}", key, opGroup);
	}

	@Override
	public void init() {
		logger.info("init");
		SolrServer.start();
		logger.info("init done");
	}

	@Override
	public void reload() {
		logger.info("reload");

	}

	@Override
	public void validateOptions() throws ConfigurationException {
		logger.info("validateOptions");

	}

	@Override
	public String getIndexName() {
		logger.info("getIndexName");
		return INDEX_NAME;
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		logger.info("createSecondaryIndexSearcher");
		return null;
	}

	@Override
	public void forceBlockingFlush() {
		logger.info("forceBlockingFlush");

	}

	@Override
	public ColumnFamilyStore getIndexCfs() {
		logger.info("forceBlockingFlush");
		return null;
	}

	@Override
	public void removeIndex(ByteBuffer columnName) {
		logger.info("removeIndex");

	}

	@Override
	public void invalidate() {
		logger.info("invalidate");

	}

	@Override
	public void truncateBlocking(long truncatedAt) {
		logger.info("truncateBlocking, trancatedAt={}", truncatedAt);

	}

	@Override
	public boolean indexes(CellName name) {
		logger.info("indexes, name={}", name);

		return false;
	}

	@Override
	public long estimateResultRows() {
		logger.info("estimateResultRows");
		return 0;
	}
}
