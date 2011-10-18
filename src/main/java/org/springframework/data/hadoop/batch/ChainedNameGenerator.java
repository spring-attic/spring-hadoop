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

import java.util.List;

/**
 * Chained name generator.
 * 
 * @author Costin Leau
 */
public class ChainedNameGenerator implements NameGenerator {

	private List<NameGenerator> generators;

	public String generate(String original) {
		String process = original;
		for (NameGenerator generator : generators) {
			process = generator.generate(process);
		}
		return process;
	}

	/**
	 * @param generators The generators to set.
	 */
	public void setGenerators(List<NameGenerator> generators) {
		this.generators = generators;
	}
}
