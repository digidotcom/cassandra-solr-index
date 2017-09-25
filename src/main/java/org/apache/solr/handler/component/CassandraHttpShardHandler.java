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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.http.client.HttpClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;

/**
 * Shard handler that determines shards based on information from Cassandra.  Currently
 * it considers any live cassandra node that is a normal state to be a shard.  Other
 * than the shard determination it works just like a typical HttpShardHandler.
 */
public class CassandraHttpShardHandler extends HttpShardHandler {
	protected static final Logger logger = LoggerFactory.getLogger(CassandraHttpShardHandler.class);
	
	/**
	 * In production this will be null which indicates we should lookup
	 * the list of available Solr nodes.  This is available in case we
	 * are running tests without Cassandra and we want to force
	 * the shard handler to talk to specific nodes.
	 */
	private static List<String> availableSolrNodeOverride = null;
	
	/**
	 * Track the token ranges each shard maps to so we can add them
	 * to the request parameters (for debug only currently, but can
	 * be used for data filtering later)
	 */
	private Map<String, List<TokenRange>> shardUrlsToTokens = new LinkedHashMap<>();
	
	/**
	 * Used during testing when not running with Cassandra to avoid the
	 * call to the storage service which would fail.
	 * @param availableSolrNodeOverride
	 */
	@VisibleForTesting
	public static void overrideAvailableSolrNodes(List<String> availableSolrNodeOverride) {
		CassandraHttpShardHandler.availableSolrNodeOverride = availableSolrNodeOverride;
	}
	
	public CassandraHttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory, HttpClient httpClient) {
		super(httpShardHandlerFactory, httpClient);
	}

	/**
	 * Add the token ranges as parameters in the request but otherwise
	 * rely on the superclass for distributing the requests
	 */
	@Override
	public void submit(ShardRequest sreq, String shard, ModifiableSolrParams params) {
		List<TokenRange> tokens = shardUrlsToTokens.get(shard);
		if(tokens != null) {
			String[] tokenRanges = tokens.stream()
					.map(tr -> tr.getStart_token() + "<->" + tr.getEnd_token())
					.toArray(size -> new String[size]);
			
			params.add("tokenRanges", tokenRanges);
		}
		
		super.submit(sreq, shard, params);
	}
	
	/**
	 * Sets the shards based on live cassandra nodes.
	 */
	@Override
	public void checkDistributed(ResponseBuilder rb) {
		SolrQueryRequest req = rb.req;
		String coreName = req.getCore().getName();
		SolrParams params = req.getParams();

		// all requests are distributed unless the distrib parameter is set to
		// false (which it is when fanning out to the nodes for processing)
		rb.isDistrib = params.getBool("distrib", true);

		if (rb.isDistrib) {
			List<String> slices = new ArrayList<>();
			List<String> shards = new ArrayList<>();

			for(Entry<String, String> shardUrls : getShards(coreName).entrySet()) {
				slices.add(shardUrls.getKey());
				shards.add(shardUrls.getValue());
			}

			rb.slices = slices.toArray(new String[slices.size()]);
			rb.shards = shards.toArray(new String[shards.size()]);
		}

		// Pulled the following out of HttpShardHandler.checkDistributed
		// since they were the only other modification to the ResponseBuilder.
		// Haven't followed through to make sure they're entirely required but
		// it seems like they just ensure a parameter from the request is
		// forwarded on so they'd make sense.
		String shards_rows = params.get(ShardParams.SHARDS_ROWS);
		if (shards_rows != null) {
			rb.shards_rows = Integer.parseInt(shards_rows);
		}
		String shards_start = params.get(ShardParams.SHARDS_START);
		if (shards_start != null) {
			rb.shards_start = Integer.parseInt(shards_start);
		}
	}
	
	/**
	 * Retrieve the available Solr nodes.  We ask Cassandra for a list
	 * of live nodes and then remove any that are currently joining, leaving
	 * or moving.  If an override list has been specified we'll return that instead
	 * of talking to Cassandra.
	 */
	public Map<String, String> getShards(String coreName) {
		if(availableSolrNodeOverride != null) {
			return availableSolrNodeOverride.stream()
				.collect(Collectors.toMap(host -> host, host -> getUrlForShard(host, coreName)));
		}
		
		StorageService storageService = StorageService.instance;
		
		/**
		 * For now we'll consider any live Cassandra node that is up
		 * and not joining/leaving/moving as an available Solr node.
		 * 
		 * This corresponds to what NodeTool Status would show as an UN (Up Normal) node
		 * https://github.com/apache/cassandra/blob/cassandra-2.1/src/java/org/apache/cassandra/tools/NodeTool.java#L2284
		 */
		List<String> liveNodes = storageService.getLiveNodes();
		
		liveNodes.removeAll(storageService.getJoiningNodes());
		liveNodes.removeAll(storageService.getMovingNodes());
		liveNodes.removeAll(storageService.getLeavingNodes());
		
		try {
			// get the token ranges and replica data for the local datacenter.  we'll sort
			// them just to make sure we iterate in a consistent order
			List<TokenRange> tokenRanges = storageService.describeLocalRing(getKeyspaceFromCoreName(coreName));
			Collections.sort(tokenRanges);
			
			for(TokenRange tokenRange : tokenRanges) {
				// the solrServer we'll use to satisfy this token range
				String shardUrl;
				
				// check if any of the replicas have already been assigned to handle
				// another token range
				Optional<String> targetedShardUrl = tokenRange.getEndpoints().stream()
					.sorted()
					.map(host -> getUrlForShard(host, coreName))
					.filter(shardUrlsToTokens::containsKey)
					.findFirst();
				
				if(targetedShardUrl.isPresent()) {
					// we found one assigned to another token range so
					// we'll use it to handle this range as well
					shardUrl = targetedShardUrl.get();
				} else {
					// none of the servers were responsible for another
					// token range...pick the first one that is alive
					Optional<String> liveServer = tokenRange.getEndpoints().stream()
						.sorted()
						.filter(liveNodes::contains)
						.findFirst();
					
					if(liveServer.isPresent()) {
						// we found a live one, assign the host
						// and populate our token range maps
						shardUrl = getUrlForShard(liveServer.get(), coreName);

						shardUrlsToTokens.put(shardUrl, new ArrayList<TokenRange>());
					} else {
						// if no live servers are available we abort the request because we
						// don't want to return a subset of the results to the client.  this
						// would only happen if all replicas for a token range are down
						throw new IllegalStateException("No replicas available for tokenRange=" + tokenRange);
					}
					
				}
				
				// add this token range to those assigned to the server
				shardUrlsToTokens.get(shardUrl).add(tokenRange);
			}
			
			// we just concatenate the token ranges together for the slice name,
			// which as far as I'm aware isn't used for anything anyway
			Map<String, String> slicesToShards = shardUrlsToTokens.entrySet().stream()
				.collect(Collectors.toMap(e -> tokenRangesToSlice(e.getValue()), e -> e.getKey()));
			
			return slicesToShards;
		} catch (InvalidRequestException e) {
			throw new IllegalArgumentException("Failure to lookup shards for coreName " + coreName, e);
		}
	}
	
	/**
	 * CoreName must be of the format {keyspace}.{tableName} otherwise
	 * an IllegalArgumentException will be thrown.
	 * @return keyspace
	 * @throws IllegalArgumentException
	 */
	private String getKeyspaceFromCoreName(String coreName) {
		List<String> parts = Splitter.on('.').splitToList(coreName);
		
		if(parts.size() != 2) {
			throw new IllegalArgumentException("Core name must be in the format {keyspace}.{table} "
					+ "but was " + coreName);
		}
		
		return parts.get(0);
	}
	
	/**
	 * Wraps the server hostname with fixed scheme, port and context
	 * and adds the coreName
	 */
	private String getUrlForShard(String host, String coreName) {
		return "http://" + host + ":8983/solr/" + coreName + "/";
	}
	
	/**
	 * Turns a list of token ranges into a string for a slice name, taking
	 * the start and end value for each range with - between them and concatenating
	 * all together with underscores.
	 */
	private String tokenRangesToSlice(List<TokenRange> tokenRanges) {
		return tokenRanges.stream()
			.map(e -> e.getStart_token() + "-" + e.getEnd_token())
			.collect(Collectors.joining("_"));
	}
}
