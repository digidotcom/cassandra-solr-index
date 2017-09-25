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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Basic tests that ensure Solr initially starts off with no documents
 * and that streams inserted into Cassandra will show up in Solr.
 */
public class IndexingTest extends CassandraInitializer {
	private static final Logger log = LoggerFactory.getLogger(IndexingTest.class);
	
	/**
	 * Verifies Solr initially has no documents and then ends up with one after
	 * one is inserted into Cassandra
	 */
	@Test
	public void testSingleInsert() throws Exception {
		// should be no documents in Solr at the start of the test
		SolrDocumentList solrDocs = getSolrServer().query(new SolrQuery("*:*")).getResults();
		Assert.assertEquals(0, solrDocs.getNumFound());

		// then we'll insert one
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, null, null, null));

		// and it should show up in solr
		solrDocs = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		);
		
		SolrDocument solrDoc = solrDocs.get(0);
		Assert.assertEquals("123-myStream", solrDoc.get("rowKey"));
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals(lastUpdatedTime, solrDoc.get("lastUpdated"));
		Assert.assertEquals("123-myStream", solrDoc.get("rowKeySearch"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
	}
	
	/**
	 * Verify that inserting many documents results in them all being indexed
	 */
	@Test
	public void testManyInserts() throws Exception {
		final long COUNT = 100;
		
		// should be no documents in Solr at the start of the test
		SolrDocumentList solrDocs = getSolrServer().query(new SolrQuery("*:*")).getResults();
		Assert.assertEquals(0, solrDocs.getNumFound());

		// then we'll insert a bunch
		LongStream.range(0, COUNT).boxed()
			.map(i -> insert(123, "myStream-" + i, i, null, null, null))
			.forEach(getSession()::execute);

		// wait for them to show up
		solrDocs = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*").setRows((int)COUNT+10)).getResults(),
			hasProperty("numFound", equalTo(COUNT))
		);
		
		// sort by lastUpdated locally to avoid testing Solr sorting
		solrDocs.sort(Comparator.comparing(doc -> (long)doc.get("lastUpdated")));
		
		for(long i = 0; i < COUNT; i++) {
			SolrDocument doc = solrDocs.get((int)i);
			
			Assert.assertEquals(i, doc.get("lastUpdated"));
			Assert.assertEquals(123, doc.get("cstId"));
			Assert.assertEquals("myStream-" + i, doc.get("streamId"));
		}
	}
	
	/**
	 * Verify inserting data in a batch results in it being indexed
	 */
	@Test
	public void testBatchInserts() throws Exception {
		final long COUNT = 100;
		
		// build insert statements and execute them in batch
		Insert[] insertStatements = LongStream.range(0, COUNT).boxed()
			.map(i -> insert(123, "myStream-" + i, i, null, null, null))
			.toArray(size -> new Insert[size]);
		
		getSession().execute(QueryBuilder.batch(insertStatements));
		
		// wait for them to show up
		SolrDocumentList solrDocs = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*").setRows((int)COUNT+10)).getResults(),
			hasProperty("numFound", equalTo(COUNT))
		);
		
		// sort by lastUpdated locally to avoid testing Solr sorting
		solrDocs.sort(Comparator.comparing(doc -> (long)doc.get("lastUpdated")));
		
		for(long i = 0; i < COUNT; i++) {
			SolrDocument doc = solrDocs.get((int)i);
			
			Assert.assertEquals(i, doc.get("lastUpdated"));
			Assert.assertEquals(123, doc.get("cstId"));
			Assert.assertEquals("myStream-" + i, doc.get("streamId"));
		}
	}
	
	/**
	 * Verifies truncating a table removes all documents from Solr
	 */
	@Test
	public void testTruncate() {
		final long COUNT = 100;
		
		// build insert statements and execute them in batch
		Insert[] insertStatements = LongStream.range(0, COUNT).boxed()
			.map(i -> insert(123, "myStream-" + i, i, null, null, null))
			.toArray(size -> new Insert[size]);
		
		getSession().execute(QueryBuilder.batch(insertStatements));
		
		// wait for them to show up
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == COUNT
		);
		
		// truncate and verify Solr eventually returns zero documents
		getSession().execute(QueryBuilder.truncate(getQuotedTable()));
		
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == 0L
		);
	}
	
	/**
	 * Verifies the index is updated when an existing row is changed
	 */
	@Test
	public void testSimpleUpdate() {
		// insert a row
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, "integer", "seconds", null));

		// verify values once it is available
		SolrDocument solrDoc = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		).get(0);
		
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertEquals("integer", solrDoc.get("dataType"));
		Assert.assertEquals("seconds", solrDoc.get("units"));
		Assert.assertNull(solrDoc.get("description"));
		
		// clear one field, update one, and add one that was not set before
		getSession().execute(insert(123, "myStream", lastUpdatedTime, null, "hours", "time stream"));
		
		// retrieve doc once its been updated
		solrDoc = await().until(
				() -> getSolrServer().query(new SolrQuery("units:hours")).getResults(),
				hasProperty("numFound", equalTo((long)1))
			).get(0);
		
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertNull(""+solrDoc.get("dataType"), solrDoc.get("dataType"));
		Assert.assertEquals("hours", solrDoc.get("units"));
		Assert.assertEquals("time stream", solrDoc.get("description"));
	}
	
	/**
	 * Mix inserts and deletes for a row and ensure the last
	 * one wins
	 */
	@Test
	public void testMixedInsertsAndDeletes() throws Exception {
		// insert and delete 10 times with the insert occurring last
		for(int i = 0; i < 10; i++) {
			getSession().execute(QueryBuilder.delete().from(getQuotedTable()).where(QueryBuilder.eq("key", "123-myStream")));
			getSession().execute(insert(123, "myStream", System.currentTimeMillis(), "integer", "seconds", null));
		}
		
		// force a commit on all solr servers so we
		// know we're not querying stale data
		for(SolrServer solrServer : getAllSolrServers()) {
			solrServer.commit();
		}
		
		// make sure we find a document in solr since we finished up with an insert
		SolrDocument solrDoc = await().until(
				() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
				hasProperty("numFound", equalTo((long)1))
			).get(0);
		
		Assert.assertEquals("123-myStream", solrDoc.get("rowKey"));
		Assert.assertEquals("seconds", solrDoc.get("units"));
		
		// next insert and delete 10 times with the delete occurring last
		for(int i = 0; i < 10; i++) {
			getSession().execute(insert(123, "myStream", System.currentTimeMillis(), "integer", "seconds", null));
			getSession().execute(QueryBuilder.delete().from(getQuotedTable()).where(QueryBuilder.eq("key", "123-myStream")));
		}
		
		// force a commit on all solr servers so we
		// know we're not querying stale data
		for(SolrServer solrServer : getAllSolrServers()) {
			solrServer.commit();
		}
		
		// make sure no documents are in solr since we did a delete last
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == 0L
		);
	}
	
	/**
	 * Verifies the index is updated when a column that does not have
	 * secondary index (but is in schema.xml) is changed.
	 */
	@Test
	public void testSingleColumnUpdate() {
		// insert a row, starting with the units equal to seconds
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, "integer", "seconds", null));

		// verify values once it is available
		SolrDocument solrDoc = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		).get(0);
		
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertEquals("integer", solrDoc.get("dataType"));
		Assert.assertEquals("seconds", solrDoc.get("units"));
		Assert.assertNull(solrDoc.get("description"));
		
		// do an insert that doesn't touch the column the secondary
		// index is defined on, switching units from seconds to hours
		getSession().execute(QueryBuilder.insertInto(getQuotedTable())
				.value("key", "123-myStream")
				.value("units", "hours"));
		
		// retrieve doc once its been updated
		solrDoc = await().until(
				() -> getSolrServer().query(new SolrQuery("units:hours")).getResults(),
				hasProperty("numFound", equalTo((long)1))
			).get(0);
		
		// verify a couple fields didn't change in the doc and
		// that the units did switch from seconds to hours
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertEquals("hours", solrDoc.get("units"));
	}
	
	/**
	 * Verifies the index is updated when a column that does not have
	 * secondary index (but is in schema.xml) is marked as null.
	 */
	@Test
	public void testSingleColumnDeletion() {
		// insert a row, starting with the units equal to seconds
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, "integer", "seconds", null));

		// verify values once it is available
		SolrDocument solrDoc = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		).get(0);
		
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertEquals("integer", solrDoc.get("dataType"));
		Assert.assertEquals("seconds", solrDoc.get("units"));
		Assert.assertNull(solrDoc.get("description"));
		
		// do an update to set the units column null where 
		// the row matches our key
		getSession().execute(QueryBuilder.update(getQuotedTable())
			.with(QueryBuilder.set("units", null))
			.where(QueryBuilder.eq("key", "123-myStream"))
		);
		
		// retrieve doc once its been updated.  this query grabs documents
		// that don't have a units field
		solrDoc = await().until(
				() -> getSolrServer().query(new SolrQuery("-units:[* TO *]")).getResults(),
				hasProperty("numFound", equalTo((long)1))
			).get(0);
		
		// verify a couple fields didn't change in the doc and
		// that the units field was removed
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertNull(solrDoc.get("units"));
	}
	
	/**
	 * Verifies a document is removed from the index when the associated
	 * row is deleted
	 */
	@Test
	public void testRowDeletion() {
		// insert a row
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, "integer", "seconds", null));

		// verify it shows up
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == 1L
		);
		
		// then delete the row
		getSession().execute(QueryBuilder.delete().from(getQuotedTable()).where(QueryBuilder.in("key", "123-myStream")));
		
		// verify it is removed from the index
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == 0L
		);
	}
	
	/**
	 * Verify the behavior when an index is added to a table
	 * that already has data
	 */
	@Test
	public void testIndexCreation() throws Exception {
		// number of rows to insert while index does not exist
		final long COUNT = 1;
		
		// get the column definitions
		List<ColumnMetadata> columns = getCluster().getMetadata().getKeyspace(getQuotedKeyspace())
			.getTable(getQuotedTable()).getColumns();

		// delete any indexes
		columns.stream()
			.map(ColumnMetadata::getIndex)
			.filter(Objects::nonNull)
			.map(IndexMetadata::getName)
			.map(Metadata::quote)
			.map(name -> "DROP INDEX " + name + ";")
			.forEach(getSession()::execute);
		
		try {
			// insert the rows in a batch
			Insert[] insertStatements = LongStream.range(0, COUNT).boxed()
				.map(i -> insert(123, "myStream-" + i, i, null, null, null))
				.toArray(size -> new Insert[size]);
			
			getSession().execute(QueryBuilder.batch(insertStatements));
			
			// solr should still show zero entries
			Assert.assertEquals(0L, getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound());
		} finally {
			// recreate the indexes
			load("DataStreamMetaDataIndex.cql");
		}
		
		// wait for data to show up
		await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults().getNumFound() == COUNT
		);
	}
	
	/**
	 * Inserting into a column not specified in schema.xml should
	 * work fine...we just won't see the field show up in solr.
	 */
	@Test
	public void testInsertingNonIndexedColumn() {
		final String NEW_COLUMN = "column_that_did_not_exist";
		
		getSession().execute("ALTER TABLE " + getQuotedTable() + " ADD " + NEW_COLUMN + " text");
		
		getSession().execute(QueryBuilder.insertInto(getQuotedTable())
				.value("key", "123-myStream")
				.value(NEW_COLUMN, "some value"));
		
		// verify values once it is available
		SolrDocument solrDoc = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		).get(0);
		
		// key should be specified but other fields
		// should not exist (including our NEW_COLUMN name
		// since it is not in schema.xml)
		Assert.assertEquals("123-myStream", solrDoc.get("rowKey"));
		Assert.assertNull(solrDoc.get(NEW_COLUMN));
		Assert.assertNull(solrDoc.get("units"));
	}
	
	/**
	 * Verify that inserting a long description will
	 * not cause an entire batch to fail
	 */
	@Test
	public void testLongDescriptionField() throws Exception {
		final String LONG_DESCRIPTION = RandomStringUtils.random(40000, true, true);

		// insert stream with long description
		long lastUpdatedTime = System.currentTimeMillis();
		getSession().execute(insert(123, "myStream", lastUpdatedTime, null, null, LONG_DESCRIPTION));

		// and it should show up in solr
		SolrDocumentList solrDocs = await().until(
			() -> getSolrServer().query(new SolrQuery("*:*")).getResults(),
			hasProperty("numFound", equalTo((long)1))
		);
		
		SolrDocument solrDoc = solrDocs.get(0);
		Assert.assertEquals("123-myStream", solrDoc.get("rowKey"));
		Assert.assertEquals(123, solrDoc.get("cstId"));
		Assert.assertEquals(lastUpdatedTime, solrDoc.get("lastUpdated"));
		Assert.assertEquals("123-myStream", solrDoc.get("rowKeySearch"));
		Assert.assertEquals("myStream", solrDoc.get("streamId"));
		Assert.assertEquals(LONG_DESCRIPTION.substring(0,  5000), solrDoc.get("description"));
	}
	
	public Insert insert(int cstId, String streamId, long lastUpdated, String dataType, String units, String description) {
    	String key = cstId + "-" + streamId;
    	String rowKeySearch = key; 	
		
		return QueryBuilder.insertInto(getQuotedTable())
    		.value("key", key)
    		.value(Metadata.quote("rowKeySearch"), rowKeySearch)
    		.value(Metadata.quote("cstId"), cstId)
    		.value(Metadata.quote("streamId"), streamId)
    		.value(Metadata.quote("lastUpdated"), lastUpdated)
    		.value(Metadata.quote("dataType"), dataType)
    		.value("units", units)
    		.value("description", description);
	}
}
