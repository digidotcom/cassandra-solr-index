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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class SolrRequest {
	
	private SolrRequestType type;
	
	private String rowKey;
	
	private Map<String, Object> fields = new HashMap<>();
	
	public SolrRequest(SolrRequestType type, String rowKey) {
		this.type = type;
		this.rowKey = rowKey;
	}
	
	public String getRowKey() {
		return rowKey;
	}
	
	public SolrRequestType getType() {
		return type;
	}
	
	public void addField(String field, Object value) {
		fields.put(field, value);
	}
	
	public Object getField(String field) {
		return fields.get(field);
	}
	
	public Map<String, Object> getAllFields() {
		return fields;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("type=").append(type).append(", rowKey=").append(rowKey);
		sb.append(", fields=").append(StringUtils.join(fields.entrySet(), ","));
		return sb.toString();
	}
}
