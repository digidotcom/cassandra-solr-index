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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * Receives requests intended for Solr, batches them up, and submits
 * them to a Solr server.
 * 
 * Submitting an item of work opens a window of time for additional work
 * to be submitted.  All the work submitted within the window will be 
 * submitted to Solr in bulk.
 * 
 */
public class SolrRequestDispatcher implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(SolrRequestDispatcher.class);
	
	private static int MAX_DRAINED_BATCH_SIZE = 1000;
	
	/**
	 * Solr request queue.  Maximum size is configurable.
	 */
	private BlockingQueue<SolrRequest> queue = new ArrayBlockingQueue<SolrRequest>(
			SolrServer.config().getMaxQueueSize());
			
	private org.apache.solr.client.solrj.SolrServer solrServer;
	
	/**
	 * A metric measuring the number of elements in the solr request
	 * queue at the time that an element is added to the queue.
	 */
	private Histogram queueDepth;
	
	/**
	 * A metric measuring the number of elements pulled out
	 * of the solr request queue and placed into a single batch
	 * for processing by the solr server.
	 */
	private Histogram batchSize;
	
	/**
	 * A metric measuring the rate at which solr processes the
	 * submitted documents in cases where the processing is 
	 * successful.  It also provides a histogram of processing
	 * times.
	 */
	private Timer solrInsertionTimer;
	
	/**
	 * A metric to track the number of solr requests that are
	 * dropped.  Requests are dropped if the queue is at capacity
	 * for longer than a specified timeout when attempting to 
	 * submit a value.
	 */
	private Counter droppedSolrRequests;
	
	/**
	 * A metric to record the number of solr documents added
	 * to the solr server and the rate they are added.
	 */
	private Meter docInsertionRate;
	
	/**
	 * A metric to track the amount of time spent waiting to insert 
	 * a request into the queue.
	 */
	private Timer queueOfferTimer;
	
	/**
	 * A metric to record the number of solr documents deleted
	 * and the rate of deletion.
	 */
	private Meter docDeletionRate;
	
	/**
	 * A metric to record the number of failed solr requests.  This
	 * counter includes failed document insertion requests as well as failed
	 * document deletion requests.  This tracks failed requests, each of which may
	 * contain multiple documents for insertion or deletion.
	 */
	private Counter failedSolrRequests;
	
	public SolrRequestDispatcher(String keyspaceName, String columnFamilyName, org.apache.solr.client.solrj.SolrServer solrServer) {
		this.solrServer = solrServer;
		setUpMetrics(keyspaceName, columnFamilyName);
	}
	
	private void setUpMetrics(String keyspace, String columnFamily) {
		String packageName = this.getClass().getPackage().getName();
		String className = this.getClass().getSimpleName();

		// Build the base MBean name
		StringBuilder baseMbeanName = new StringBuilder();
		baseMbeanName.append(packageName).append(":");
		baseMbeanName.append("type=").append(className);
		baseMbeanName.append(",keyspace=").append(keyspace);
		baseMbeanName.append(",scope=").append(columnFamily);

		/*
		 * build the Histogram to measure solr request queue depth
		 */
		MetricName qDepthMetricName = createMetricName(keyspace, columnFamily, baseMbeanName,
				"IndexQueueDepth");
		this.queueDepth = Metrics.newHistogram(qDepthMetricName, true);

		/* 
		 * build the Histogram to measure the size of batches created for
		 * submitting to solr
		 */
		MetricName batchSizeMetricName = createMetricName(keyspace, columnFamily, baseMbeanName,
				"IndexBatchSize");
		this.batchSize = Metrics.newHistogram(batchSizeMetricName, true);

		/*
		 * build the timer to measure rate and duration of requests to
		 * solr server
		 */
		MetricName insertMetricName = createMetricName(keyspace, columnFamily, baseMbeanName,
				"SolrInsertionTimer");
		this.solrInsertionTimer = Metrics.newTimer(insertMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		
		/*
		 * build the counter to track number of dropped solr requests 
		 */
		MetricName droppedRequestName = createMetricName(keyspace, columnFamily, baseMbeanName,
				"DroppedSolrRequests");
		this.droppedSolrRequests = Metrics.newCounter(droppedRequestName);
		
		/*
		 * build the meter to track the solr document insertion rate
		 */
		MetricName insertionRateName = createMetricName(keyspace, columnFamily, baseMbeanName, "SolrDocInsertionRate");
		this.docInsertionRate = Metrics.newMeter(insertionRateName, "documents", TimeUnit.SECONDS);

		/*
		 * build the timer to track rate and duration of attempts to add requests to
		 * the queue
		 */
		MetricName queueOfferName = createMetricName(keyspace, columnFamily, baseMbeanName, "QueueOfferTime");
		this.queueOfferTimer = Metrics.newTimer(queueOfferName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

		/*
		 * build the meter to track the solr document deletion rate
		 */
		MetricName deletionRateName = createMetricName(keyspace, columnFamily, baseMbeanName, "SolrDocDeletionRate");
		this.docDeletionRate = Metrics.newMeter(deletionRateName, "documents", TimeUnit.SECONDS);

		/*
		 * build the counter to track failed solr requests
		 */
		MetricName failedSolrReqName = createMetricName(keyspace, columnFamily, baseMbeanName, "FailedSolrRequests");
		this.failedSolrRequests = Metrics.newCounter(failedSolrReqName);
	}
	
	private MetricName createMetricName(String keyspace, String columnFamily,
			StringBuilder baseMbeanName, String name) {
		String packageName = this.getClass().getPackage().getName();
		String className = this.getClass().getSimpleName();
		
		StringBuilder qDepthMbeanName = new StringBuilder(baseMbeanName);
		qDepthMbeanName.append(",name=").append(name);
		MetricName metricName = new MetricName(packageName, className, name, keyspace + "." + columnFamily,
				qDepthMbeanName.toString());

		return metricName;
	}
	
	/**
	 * Submit an item of work for the Solr server.
	 * 
	 * @param solrRequest an item of work for the Solr server
	 */
	public void submit(SolrRequest solrRequest) {
		logger.debug("Adding a request to the queue={}", solrRequest);

		try {
			// Offer the request to the queue and record the time in the metric.
			final TimerContext context = queueOfferTimer.time();
			boolean successfulOffer = queue.offer(solrRequest, SolrServer.config().getQueueMaxOfferDuration(),
					TimeUnit.MILLISECONDS);
			context.stop();
			
			if (successfulOffer) {
				
				// Update metric tracking size of the solr request queue
				queueDepth.update(queue.size());
				
			} else {
				
				// Failed to put the request in the queue after waiting the specified time.
				// Update metric tracking dropped requests
				droppedSolrRequests.inc();
				logger.warn("Dropping solr request for rowKey={} because queue is at capacity",
						solrRequest.getRowKey());
			}
		} catch (InterruptedException e) {
            logger.error("Interrupted while waiting to insert a solr request into the queue", e);
		}
	}

	@Override
	public void run() {

		while (true) {
			try {
				// Take a request from the queue.
				List<SolrRequest> batch = new ArrayList<>();
				logger.trace("Taking a request from the queue");
				SolrRequest request = queue.take();
				logger.trace("Took a request from the queue={}" + request);
				batch.add(request);

				if (queue.size() + 1 < SolrServer.config().getImmediateBatchThreshold()) {
					// Wait a small-ish amount of time to allow additional requests
					// to flow in to the queue.
					Thread.sleep(SolrServer.config().getBatchWindowDuration());
				}
				
				// Drain additional requests that have flowed in, and account
				// for request already removed in the take above.
				queue.drainTo(batch, MAX_DRAINED_BATCH_SIZE - 1);
				
				// Update metric tracking size of batches.
				batchSize.update(batch.size());
				logger.debug("Drained a total of {} requests from the queue", batch.size());

				List<SolrInputDocument> docsToAdd = new ArrayList<>();

				// List of rowKeys to be removed from Solr
				List<String> docsToDelete = new ArrayList<>();

				// Group requests that are for the same row.
				Map<String, List<SolrRequest>> groupedRequests = groupByRow(batch);

				// Merge requests for the same row into a single Solr document.
				groupedRequests.entrySet().forEach((entry) -> {
					String rowKey = entry.getKey();
					List<SolrRequest> requestsForRow = entry.getValue();
					Optional<SolrInputDocument> solrDoc = createDocument(requestsForRow);
					if (solrDoc.isPresent()) {
						docsToAdd.add(solrDoc.get());
					} else {
						docsToDelete.add(rowKey);
					}

				});

				// Send the batch of Solr documents to Solr.
				if (!docsToAdd.isEmpty()) {
					try {
						int numDocsToAdd = docsToAdd.size();

						// Submit documents to solr and update Timer metric
						final TimerContext context = solrInsertionTimer.time();
						UpdateResponse response = solrServer.add(docsToAdd);
						context.stop();
						
						// update the meter to account for the number of docs added
						docInsertionRate.mark(numDocsToAdd);
						
						logger.debug("response from add={}", response);
					} catch (Exception e) {
						failedSolrRequests.inc();
						logger.error("Failed to add documents to solr server", e);
					}
				}

				if (!docsToDelete.isEmpty()) {
					try {
						int numDocsToDelete = docsToDelete.size();
						
						// batch delete documents
						UpdateResponse response = solrServer.deleteById(docsToDelete);
						
						// update meter to account for the number of docs deleted
						docDeletionRate.mark(numDocsToDelete);
						
						logger.debug("response from delete={}", response);
					} catch (Exception e) {
						failedSolrRequests.inc();
						logger.error("Failed to delete documents from solr server", e);
					}
				}
			} catch (Exception e) {
				logger.warn("Caught unexpected exception while dispatching solr requests", e);
			}
		}
	}
	
	/**
	 * Take a list of queued SolrRequests and group them by their row keys.
	 * 
	 * @param queuedRequests List of SolrRequests that have been queued
	 * @return a Map containing SolrRequests grouped by their rowKey
	 */
	private Map<String, List<SolrRequest>> groupByRow(List<SolrRequest> queuedRequests) {
		return queuedRequests.stream().collect(Collectors.groupingBy(SolrRequest::getRowKey));		
	}
	
	/**
	 * Merge the SolrRequests for a single rowKey into a single SolrInputDocument.  
	 * The last update will overwrite previous updates in cases where there is
	 * more than 1 update to the same field.
	 * 
	 * @param a List of SolrRequests where all the requests have the same rowKey
	 * @return SolrInputDocument containing all fields found within the SolrRequests
	 */
	private Optional<SolrInputDocument> createDocument(List<SolrRequest> requestForSingleRow) {
		if (requestForSingleRow.isEmpty()) {
			return Optional.empty();
		}
		
		SolrInputDocument solrDoc = new SolrInputDocument();
		String rowKey = requestForSingleRow.get(0).getRowKey();

		// Treating all changes as partial updates since we aren't doing a 
		// read for the SolrDocument before making changes.
		for (SolrRequest solrRequest : requestForSingleRow) {
			switch (solrRequest.getType()) {
				case UPSERT:
					solrDoc.setField("rowKey", rowKey);
					Map<String, Object> allFields = solrRequest.getAllFields();
					Iterator<Entry<String, Object>> iter = allFields.entrySet().iterator();
					while (iter.hasNext()) {
						
						Entry<String, Object> entry = iter.next();
						String fieldName = entry.getKey();
						
						// Don't want to update the rowKey.
						// Updating the document's key via the partial
						// update semantic is not supported and results
						// in an exception when the document is added to solr.
						if (fieldName.equals("rowKey")) {
							continue;
						}
						Object fieldValue = entry.getValue();
						solrDoc.setField(fieldName, Collections.singletonMap("set", fieldValue));
					}
					break;
				case DELETE:
					// A delete will effectively wipe away previous updates.
					// If updates came in the same batch after the delete, they will
					// update a fresh document for the same rowKey.
					solrDoc = new SolrInputDocument();
					break;
			}
			
		}
		return solrDoc.isEmpty() ? Optional.empty() : Optional.of(solrDoc);
	}

}
