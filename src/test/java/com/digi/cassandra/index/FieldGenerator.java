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

import java.util.Map;
import java.util.Random;

public class FieldGenerator {
	
	private Map<String, String> nameToTypeMap;
	
	private Random random = new Random();
	
	public FieldGenerator(Map<String, String> nameToTypeMap) {
		this.nameToTypeMap = nameToTypeMap;
	}
	
	public Object generateValue(String name) {
		String type = nameToTypeMap.get(name);
		if (type.equalsIgnoreCase("text")) {
			return name + "Value" + random.nextInt(1000);
		} else if (type.equals("int")) {
			return random.nextInt(1000);
		} else if (type.equals("bigint")){
			return new Long(random.nextInt(1000));
		} else {
			throw new IllegalArgumentException(name + " is a currently unsupported type: " + type);
		}
	}

}
