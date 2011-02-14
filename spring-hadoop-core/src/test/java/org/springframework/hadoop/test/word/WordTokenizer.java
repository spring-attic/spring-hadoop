/*
 * Copyright 2006-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.hadoop.test.word;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author Dave Syer
 * 
 */
public class WordTokenizer implements CustomTokenizer {

	public Iterable<String> tokenize(String input) {
		List<String> list = new ArrayList<String>();
		StringTokenizer outer = new StringTokenizer(input);
		while (outer.hasMoreTokens()) {
			String token = outer.nextToken();
			token = token.toLowerCase().replaceAll("[^a-z]", " ");
			StringTokenizer inner = new StringTokenizer(token.trim());
			while (inner.hasMoreTokens()) {
				list.add(inner.nextToken());
			}
		}
		return list;
	}

}
