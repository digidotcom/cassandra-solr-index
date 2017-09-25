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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexer extends PerRowSecondaryIndex {

	private static final Logger logger = LoggerFactory.getLogger(SolrIndexer.class);

	private HttpSolrServer solrServer;

	private String indexName;

	private Set<String> solrFields;
	
	private SolrRequestDispatcher solrRequestDispatcher;

	/**
	 * Write or remove a document to/from Solr for the row provided.  Only columns with defined indexes are written.
	 *
	 * This method does make an assumption about the underlying Solr schema and the C* schema the may preclude the class
	 * from being used to generically index documents to Solr.  Specifically, it writes the C* rowKey to a field with the
	 * name of rowKey. It assumes the rowKey is a String, and that it is the only column used as the primary key.
	 */ 
	@Override
	public void index(ByteBuffer rowKey, ColumnFamily cf) {
		logger.debug("index, columnFamily={}", this.hashCode(), cf);

		try {
			// decode the rowKey to a String
			String rowKeyString = ByteBufferUtil.string(rowKey);
			logger.debug("rowKey to index : " + rowKeyString);

			Iterator<Cell> iter = cf.iterator();
			if (iter.hasNext()) {
				
				// Build an insert/update request
				SolrRequest solrRequest = new SolrRequest(SolrRequestType.UPSERT, rowKeyString);
				solrRequest.addField("rowKey", rowKeyString);

				// Gather all the fields that need to be added to the Solr document.
				while (iter.hasNext()) {
					Cell cell = iter.next();
					CellName name = cell.name();
										
					// name of the column or skip if it's not a name available via CQL
					ColumnIdentifier columnId = name.cql3ColumnName(cf.metadata());
					if(columnId == null) {
						continue;
					}
					String columnName = columnId.toString();
					
					if(!solrFields.contains(columnName)) {
						// skip if this column isn't in the solr schema
						continue;
					}
					
					logger.debug("column requires indexing={}", columnName);
					
					if(!cell.isLive()) {
						// cell is dead, its new value is null
						solrRequest.addField(columnName, null);
						continue;
					}
					
					ByteBuffer cellByteBuffer = cell.value();
					try {
						TypeSerializer<?> serializer = cf.metadata().getColumnDefinition(name).type.getSerializer();
						serializer.validate(cellByteBuffer);
						Object fieldValue = serializer.deserialize(cellByteBuffer);
						if(fieldValue instanceof String) {
							if(((String) fieldValue).length() > 5000) {
								logger.warn("Field=" + columnName + " truncated for rowKey=" + rowKeyString);
								fieldValue = ((String) fieldValue).substring(0, 5000);
							}
						}
						
						solrRequest.addField(columnName, fieldValue);
						
					} catch (MarshalException e) {
						byte dump[] = new byte[Math.min(64, cellByteBuffer.remaining())];
						cellByteBuffer.get(dump);
						logger.warn(
								"Could not decode value for {} column.  This field will not be added to Solr document. Partial value=0x{}",
								columnName, Hex.bytesToHex(dump));
					}
				}
				logger.debug("solrRequest={}", solrRequest);
				solrRequestDispatcher.submit(solrRequest);
			} else if (cf.deletionInfo() != null) {
				// If we made it into this block, we are processing the deletion of an entire row.
				logger.debug("deleting row with key={}", rowKeyString);
				SolrRequest deleteRequest = new SolrRequest(SolrRequestType.DELETE, rowKeyString);
				solrRequestDispatcher.submit(deleteRequest);
			}
		} catch (CharacterCodingException e) {
			logger.warn("Could not decode rowKey", e);
		} catch (Exception e) {
			logger.error("Unexpected exception while indexing", e);
		}
	}

	@Override
	public void delete(DecoratedKey key, Group opGroup) {
		logger.debug("delete, key={}, opGroup={}", key, opGroup);
	}

	@Override
	public void init() {
		// This index will be initialized at a time when there is only 1 ColumnDefinition in the
		// columnDefs set.  Other columns will be added if the table has additional columns using this
		// index.  Those additions will happen after the call to init().
		//
		// Additionally, this appears to be invoked only once no matter how many times this class
		// is used as an index class for different indexes defined in CQL.
		//
		// The following comment is pasted here for some context on how Cassandra will interact with this
		// index.  It was taken from SecondaryIndexManager.addIndexedColumn() line 290 at the time of this
		// writing.
		//
		// "Keep a single instance of the index per-cf for row level indexes
        // since we want all columns to be under the index."
		ColumnDefinition columnDefinition = columnDefs.iterator().next();
		logger.debug("number of columnDefinitions={}", columnDefs.size());
		
		indexName = columnDefinition.getIndexName();

		logger.info("Initializing SolrIndexer with indexName={}", indexName);

		SolrServer.start(); // Idempotent
		SolrIndexConfiguration config = SolrServer.config();

		String keyspaceName = baseCfs.keyspace.getName();
		String tableName = baseCfs.getColumnFamilyName();

		final String coreName = keyspaceName + "." + tableName;
		// might need some flexibility here to set host and port values
		String solrUrl;
		if (config == null) {
			solrUrl = "http://localhost:8983/solr";
			logger.error("During initialization, SolrServer didn't seem to start correctly. Defaulting to: {}",
			             solrUrl);
		}
		else {
			solrUrl = "http://"
					+ (config.getSolrAddress().getHostAddress().equals("0.0.0.0") ? "127.0.0.1"
							: config.getSolrAddress().getHostAddress())
					+ ":" + config.getSolrPort() + "/" + config.getSolrContext();
		}
		solrServer = new HttpSolrServer(solrUrl);

		logger.info("Using URL for connecting to Solr server: {}", solrUrl);
		
		try {
			createCoreIfNeeded(coreName);
		} catch (Exception e) {
			logger.error("Failed to find or create core=" + coreName + " which may indicate "
					+ "a critical failure preventing data from getting indexed", e);
		}
		
		// adjust url as subsequent requests will be scoped to the core
		solrServer.setBaseURL(solrServer.getBaseURL() + "/" + coreName);

		/*
		 * Get the fields that have been defined in the solr schema.
		 * Use these to ensure we only insert data into fields
		 * that have been defined by the schema.
		 */
		this.solrFields = new HashSet<>();
		SolrQuery query = new SolrQuery();
	    query.add(CommonParams.QT, "/schema/fields");
		try {
			QueryResponse response = solrServer.query(query);
			@SuppressWarnings({ "unchecked" })
			ArrayList<SimpleOrderedMap<Object>> fields = (ArrayList<SimpleOrderedMap<Object>>) response.getResponse().get("fields");
			for (SimpleOrderedMap<Object> field : fields) {
				String fieldName = (String) field.get("name");
				this.solrFields.add(fieldName);
			}
		} catch (SolrServerException e) {
			logger.error("Unable to query Solr server at {}.  Solr fields will not be indexed." + solrUrl, e );
		}
		
		this.solrRequestDispatcher = new SolrRequestDispatcher(keyspaceName, tableName, solrServer);
		Thread t = new Thread(solrRequestDispatcher, "SolrRequestDispatcher-" + keyspaceName + "-" + tableName);
		t.start();
	}

	@Override
	public void reload() {
		logger.debug("reload");

	}

	@Override
	public void validateOptions() throws ConfigurationException {
		logger.debug("validateOptions");

	}

	@Override
	public String getIndexName() {
		logger.debug("getIndexName {}", indexName);
        return indexName;
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		logger.debug("createSecondaryIndexSearcher");
		return null;
	}

	@Override
	public void forceBlockingFlush() {
		logger.debug("forceBlockingFlush");

	}

	@Override
	public ColumnFamilyStore getIndexCfs() {
		logger.debug("forceBlockingFlush");
		return null;
	}

	@Override
	public void removeIndex(ByteBuffer columnName) {
		try {
			String colNameString = ByteBufferUtil.string(columnName);
			logger.debug("removeIndex, columnName={}", colNameString);
		} catch (CharacterCodingException e) {
			logger.warn("Could not decode columnName during removeIndex");
		}
	}

	@Override
	public void invalidate() {
		logger.debug("invalidate");

	}

	/**
	 * Delete all documents from Solr without regard for the time they were inserted.
	 *
	 */
	@Override
	public void truncateBlocking(long truncatedAt) {
		logger.info("SolrIndexer, truncateBlocking, trancatedAt={}", truncatedAt);
		try {
			// Currently deleting all documents stored in this solr core.  We could
			// do more targeting deleting with some additional changes.
			// One possibility is to use the lastUpdated field that is already in
			// the solr schema.  We could run a query to find all records that have
			// lastUpdated values less than the truncatedAt value.  Unfortunately,
			// range queries on that field do not work as one would expect.
			// The field is stored as an org.apache.solr.schema.Long field, which is
			// evaluated as a unicode String when used in a range query.  See javadoc
			// here https://lucene.apache.org/solr/4_10_3/solr-core/org/apache/solr/schema/LongField.html
			// We could explore migrating the field to a TrieLongField to get the range
			// query working, but deleting all documents for now.
			//
			// Sample query if we decide to use ranges:
			// String deleteQuery = "lastUpdated:[* TO " + truncatedAt + "]";

			solrServer.deleteByQuery("*:*");
			// solrServer.commit();
			//
		} catch (SolrServerException | IOException e) {
			logger.warn("Truncate failed to removed data from Solr", e);
		}

	}

	/**
	 * Indicate the columns we're willing to index.  Our indexing is
	 * based on the solr schema.xml so we'll use that to evaluate anything
	 * that is passed in and index it if it matches the name of an index
	 * solr field.
	 */
	@Override
	public boolean indexes(CellName name) {
		ColumnIdentifier columnId = name.cql3ColumnName(getBaseCfs().metadata);
		if(columnId == null) {
			// we don't index columns that don't have a cql3 name available
			logger.debug("Failed to retrieve column name for cell={}", name);
			return false;
		} else {
			String columnName = columnId.toString();
			logger.trace("indexes columnName={}", columnName);
			
			return solrFields.contains(columnName);
		}
	}

	@Override
	public long estimateResultRows() {
		logger.debug("SolrIndexer, estimateResultRows");
		return 0;
	}
	
	/**
	 * Checks to see if the specified coreName exists and creates it if it does not
	 */
	private void createCoreIfNeeded(String coreName) throws IOException, SolrServerException {
		CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, solrServer);
		boolean coreExists = statusResp.getStartTime(coreName) != null;
		
		if(coreExists) {
			logger.info("Found core={}", coreName);
		} else {
			logger.info("Core={} not found, creating it", coreName);
			
			CoreAdminRequest.Create req = new CoreAdminRequest.Create();
			req.setCoreName(coreName);
			req.setConfigSet(SolrConfigInitializer.DATASTREAM_V1_CONFIGSET);
			
			CoreAdminResponse createResp = req.process(solrServer);
			logger.info("Core={} creation resulted in: {}", coreName, createResp);
		}
	}
}
