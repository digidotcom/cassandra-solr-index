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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.digi.cassandra.index.server.CassandraInitializer;
import com.jayway.awaitility.Awaitility;

/*
 * This test doesn't have any logic to auto setup the CQL keyspace, table, or indexes.
 * At this point, it is assumed that you have set them up on your own.  It uses
 * the DataStreamMetaData table and its indexes.
 *
 */
public class SolrIndexerTest extends CassandraInitializer {
	
	private final static String CQL_TABLE = "\"" + getTableName() + "\"";
	
	/*
	 * 7199 and 7100 are common choices.  Check the cassandra-env.sh.
	 */
	private final static int CASSANDRA_JMX_PORT = 7199;
	
	private static List<ColumnMetadata> columnMetadataList;

	private final static Map<String, String> columnNameToTypeMap = new HashMap<>();

	private static FieldGenerator fieldGenerator;

	private final static Set<String> excludedColumns = new HashSet<>();

	@BeforeClass
	public static void beforeClass() throws SolrServerException, IOException {
	    TableMetadata table = getCluster().getMetadata().getKeyspace(getQuotedKeyspace()).getTable(getQuotedTable());
	    columnMetadataList = table.getColumns();

	    for (ColumnMetadata columnMetadata: columnMetadataList) {
	    	String columnName = columnMetadata.getName();
	    	String columnType = columnMetadata.getType().toString();

	    	columnNameToTypeMap.put(columnName, columnType);
	    }
	    fieldGenerator = new FieldGenerator(columnNameToTypeMap);

	    excludedColumns.add("_docBoost");
	    excludedColumns.add("rowKeySearch");
	    excludedColumns.add("solr_query");
	    excludedColumns.add("currentValue");
	    excludedColumns.add("forwardTo");

	}



	@Test
	public void testFullRowInsertsAndRemovals() throws SolrServerException {
		final int numRowsToGenerate = 10;

		List<Map<String, Object>> rowsInserted = insertRows(numRowsToGenerate,
				columnNameToTypeMap.keySet().toArray(new String[columnNameToTypeMap.size()]));

		validateRows(rowsInserted, true);

		removeRow(rowsInserted, true);

	}

	@Test
	public void testRowUpdates() {
		final int numRowsToGenerate = 100;

		Set<String> columnsForCreation = new HashSet<>();
		columnsForCreation.add("key");
		columnsForCreation.add("cstId");
		columnsForCreation.add("dataType");

		List<Map<String, Object>> rows = insertRows(numRowsToGenerate,
				columnsForCreation.toArray(new String[columnsForCreation.size()]));

		Set<String> columnsToUpdate = new HashSet<>();
		columnsToUpdate.add("dataType");
		columnsToUpdate.add("units");
		columnsToUpdate.add("lastUpdated");
		rows = updateRows(rows, columnsToUpdate.toArray(new String[columnsToUpdate.size()]));

		validateRows(rows, true);

	}

	@Test
	public void testBasicTruncate() throws SolrServerException {
		insertRows(10, columnNameToTypeMap.keySet().toArray(new String[columnNameToTypeMap.size()]));

		// truncate the table
		getSession().execute(QueryBuilder.truncate(CQL_TABLE));

		// all data should be gone from Solr
		validateSolrIsEmpty();
	}

	/*
	@Test
	public void testConcurrentUpdates() {
		List<Map<String, Object>> rows = insertRows(1, columnNameToTypeMap.keySet().toArray(new String[columnNameToTypeMap.size()]));

		Set<String> columnsToUpdate = new HashSet<>();
		columnsToUpdate.add("dataType");
		columnsToUpdate.add("units");
		columnsToUpdate.add("lastUpdated");
		updateRows(rows, columnsToUpdate.toArray(new String[columnsToUpdate.size()]));
		updateRows(rows, columnsToUpdate.toArray(new String[columnsToUpdate.size()]));


	}
	*/

	@Test
	public void testBasicIndexRebuild() throws Exception {
		// insert rows into cassandra
		final int numRows = 10;
		List<Map<String, Object>> rows = insertRows(numRows, columnNameToTypeMap.keySet().toArray(new String[columnNameToTypeMap.size()]));
		validateRows(rows, true);

		// remove half of the documents from Solr
		int numSolrDocsRemoved = 0;
		for (int i=0; i < numRows; i++) {
			if (i % 2 == 0) {
				Map<String, Object> row = rows.get(i);
				String rowKey = (String) row.get("key");
				removeSolrDocuemnts(Collections.singleton(rowKey));
				numSolrDocsRemoved++;
			}
		}

		// verify that cassandra has the expected number of rows
		ResultSet result = getSession().execute(QueryBuilder.select().countAll().from(CQL_TABLE));
		Row row = result.one();
		long count = row.getLong("count");
		Assert.assertEquals("Did not find the expected number of cassandra rows", numRows, count);

		QueryResponse response = getSolrServer().query(new SolrQuery("*:*"));
		Assert.assertEquals("Did not find the expected number of Solr documents", numRows - numSolrDocsRemoved, response.getResults().getNumFound());

		// will need to flush here to get the test to work.
		// could be a bug with cassandra where rebuild index only iterates through SSTable rows
		// and not the memtable rows
		List<JMXConnector> jmxcs = createJMXConnectors();
		flush(jmxcs);

		// rebuilding the secondary index should repopulate the Solr documents.
		rebuildSecondaryIndexes(jmxcs);

		Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
			try {
				return numRows == getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound();
			} catch (SolrServerException e) {
				e.printStackTrace();
				return false;
			}
		});
	}
	
	@Test
	public void testMultipleDeletionsOfSingleRow() {
		final int numRowsToGenerate = 100;

		List<Map<String, Object>> rowsInserted = insertRows(numRowsToGenerate,
				columnNameToTypeMap.keySet().toArray(new String[columnNameToTypeMap.size()]));

		validateRows(rowsInserted, true);
		
		// Delete the same row multiple times just to verify that the resulting
		// request to delete the Solr document is idempotent.  
		Map<String, Object> rowToRemove = rowsInserted.remove(0);
		for (int i=0; i < 50; i++) {
			removeRow(Collections.singletonList(rowToRemove), false);
		}
		
		// validate the row has been removed
		validateRemovalFromSolr((String) rowToRemove.get("rowKey"), 2000);
		
		// validate that all other rows remain
		validateRows(rowsInserted, true);
	}


	private void removeSolrDocuemnts(Set<String> rowKeys) throws SolrServerException, IOException {
		for (String rowKey : rowKeys) {
			getSolrServer().deleteByQuery("rowKey:" + rowKey);
		}
		getSolrServer().commit();
	}


	private List<Map<String, Object>> insertRows(int numRowsToInsert, String... columnsToInclude) {
		List<Map<String, Object>> rowsInserted = new ArrayList<>();
		for (int i=0; i < numRowsToInsert; i++) {
			// key is field name.  value is field value
			Map<String, Object> insertedColumns = new HashMap<>();

			Insert insert = QueryBuilder.insertInto(CQL_TABLE);
			for (String columnName : columnsToInclude) {
				if (excludedColumns.contains(columnName)) {
					continue;
				}

				Object value;
				if (columnName.equals("key")) {
					value = "keyValue" + i;
				} else {
					value = fieldGenerator.generateValue(columnName);
				}
				insert.value("\"" + columnName + "\"", value);

				insertedColumns.put(columnName, value);
				System.out.println("inserted " + columnName + ":" + value);

			}

			rowsInserted.add(insertedColumns);

			getSession().execute(insert);

		}
		return rowsInserted;
	}

	private List<Map<String, Object>> updateRows(List<Map<String, Object>> existingRows, String... columnsToUpdate) {
		existingRows.forEach((row) -> {
			Insert insert = QueryBuilder.insertInto(CQL_TABLE);
			insert.value("key", row.get("key"));
			for (String columnName : columnsToUpdate) {
				if (excludedColumns.contains(columnName)) {
					continue;
				}
				Object value = fieldGenerator.generateValue(columnName);
				insert.value("\"" + columnName + "\"", value);
				row.put(columnName, value);
				System.out.println("updating " + columnName + "=" + value);
			}
			getSession().execute(insert);
		});

		return existingRows;
	}

	/**
	 * @param rows expected values
	 * @param validateAllColumns
	 */
	private void validateRows(List<Map<String, Object>> rows, boolean validateAllColumns) {
		for (Map<String, Object> row : rows) {
			String key = (String) row.get("key");
			SolrQuery solrQuery = new SolrQuery("rowKey:" + key);

			System.out.println("key=" + key);

			Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
				QueryResponse response = getSolrServer().query(solrQuery);
				SolrDocumentList solrDocList = response.getResults();
				if (solrDocList.size() != 1) {
					return false;
				}

				SolrDocument solrDoc = solrDocList.get(0);

				// row.entrySet().parallelStream().allMatch(predicate)

				boolean allFieldsVerified = row.entrySet().stream().allMatch((entry) -> {
					String expectedFieldName = entry.getKey();
					expectedFieldName = expectedFieldName.equals("key") ? "rowKey" : expectedFieldName;
					Object expectedFieldValue = entry.getValue();

					Object solrValue = solrDoc.get(expectedFieldName);
					// Awaitility.await()entry.

					if (!expectedFieldValue.equals(solrValue)) {
						System.out.println(expectedFieldName + " had a value of " + solrValue
								+ ". Which did not equal expected value of " + expectedFieldValue);
					}
					return expectedFieldValue.equals(solrValue);
				});
				return allFieldsVerified;
			});
		}
	}

	private void removeRow(List<Map<String, Object>> rowsToRemove, boolean verifyRemoval) {
		rowsToRemove.forEach((row) -> {
			String key = (String) row.get("key");
			Where where = QueryBuilder.delete().from(CQL_TABLE).where(QueryBuilder.eq("key", key));
			getSession().execute(where);
			if (verifyRemoval) {
				validateRemovalFromSolr(key, 2000);
			}
		});
	}
	/**
	 *
	 * @param value validating that the row for this value is no longer present.
	 * @param waitTimeMs amount of time to wait between attempts in milliseconds
	 */
	private void validateRemovalFromSolr(String value, long waitTimeMs) {
		Awaitility.await().atMost(waitTimeMs, TimeUnit.MILLISECONDS).until(() -> {
			boolean documentFound = false;
			SolrQuery solrQuery = new SolrQuery("rowKey:" + value);
			QueryResponse response = getSolrServer().query(solrQuery);
			System.out.println("validateRemovalFromSolr queryResponse=" + response);
			documentFound = response.getResults().stream()
					.anyMatch((document) -> document.getFieldValue("rowKey").equals(value));

			return !documentFound;
		});
	}

	private void validateSolrIsEmpty() throws SolrServerException {
		Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
			SolrQuery solrQuery = new SolrQuery("*:*");
			QueryResponse response = getSolrServer().query(solrQuery);
			SolrDocumentList solrDocList = response.getResults();
			return solrDocList.isEmpty();
		});
	}

	public List<JMXConnector> createJMXConnectors() throws IOException {
		return getCluster().getMetadata().getAllHosts().stream()
			.map(Host::getAddress)
			.map(InetAddress::getHostAddress)
			.map(host -> "service:jmx:rmi:///jndi/rmi://" + host + ":" + CASSANDRA_JMX_PORT + "/jmxrmi")
			.map(this::getJmxConnector)
			.collect(Collectors.toList());
	}


	public void rebuildSecondaryIndexes(List<JMXConnector> jmxcs) throws IOException, MalformedObjectNameException, InstanceNotFoundException, MBeanException, ReflectionException  {
		for(JMXConnector jmxc : jmxcs) {
			MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
			Set<ObjectName> names = new TreeSet<ObjectName>(
					mbsc.queryNames(new ObjectName("org.apache.cassandra.db:type=StorageService"), null));
			ObjectName name = names.iterator().next();
				
			final String KEYSPACE = getKeyspace();
			final String TABLE = getTableName();
			
			mbsc.invoke(name, "rebuildSecondaryIndex", new Object[] { KEYSPACE, TABLE,
						new String[] { KEYSPACE + "_DataStreamMetaData_rowKeySearch_index" } },
						new String[] { String.class.getName(), String.class.getName(), String[].class.getName() });
		}
	}
	
	private void flush(List<JMXConnector> jmxcs) throws IOException, MalformedObjectNameException, InstanceNotFoundException, MBeanException, ReflectionException {
		for(JMXConnector jmxc : jmxcs) {
			MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
			Set<ObjectName> names = new TreeSet<ObjectName>(
					mbsc.queryNames(new ObjectName("org.apache.cassandra.db:type=StorageService"), null));
			ObjectName name = names.iterator().next();
			mbsc.invoke(name, "forceKeyspaceFlush", new Object[] { getKeyspace(), new String[]{} },
					new String[] { String.class.getName(), String[].class.getName() });
		}
	}

	public JMXConnector getJmxConnector(String url) {
		try {
			JMXServiceURL serviceURL = new JMXServiceURL(url);
			return JMXConnectorFactory.connect(serviceURL, null);
		} catch (IOException e) {
			throw new RuntimeException("Failed to connect to JMX", e);
		}
	}
}
