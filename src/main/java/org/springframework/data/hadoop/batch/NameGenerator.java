/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.batch;

/**
 * Strategy for generating names. Acts usually as a translator for names and identifiers
 * between different environments.
 * 
 * @author Costin Leau
 */
public interface NameGenerator {

	/**
	 * Generates a name based on the given input.
	 * 
	 * @param original original name 
	 * @return translated/generated name based on input
	 */
	String generate(String original);
}
