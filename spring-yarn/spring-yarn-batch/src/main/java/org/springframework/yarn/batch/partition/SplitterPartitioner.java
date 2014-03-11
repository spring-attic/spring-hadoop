/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.batch.partition;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.data.hadoop.store.split.Splitter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link Partitioner} that locates multiple resources and
 * associates their file names with execution context keys.
 * <p>
 * Creates an {@link ExecutionContext} per resource, and labels them as
 * <code>{partition0, partition1, ..., partitionN}</code> where 'partition' part
 * comes from a {@link #getPartitionBaseIdentifier()}.
 * <p>
 * The grid size information passed to method {@link Partitioner#partition(int)}
 * is ignored.
 *
 * @author Janne Valkealahti
 *
 */
public class SplitterPartitioner extends AbstractPartitioner {

	private Splitter splitter;

	private Set<String> inputPatterns;

	@Override
	protected Map<String, ExecutionContext> createPartitions() {
		Map<String, ExecutionContext> contexts = new HashMap<String, ExecutionContext>();

		try {
			int i = 0;
			for (Resource resource : resolveResources()) {
				Assert.state(resource.exists(), "Resource does not exist: " + resource);
				List<Split> inputSplits = splitter.getSplits(new Path(resource.getURI()));
				for (Split split : inputSplits) {
					contexts.put(getPartitionBaseIdentifier() + i++, createExecutionContext(resource, split));
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error partitioning splits", e);
		}

		return contexts;
	}

	/**
	 * Sets the input patterns.
	 *
	 * @param inputPatterns the new input patterns
	 */
	public void setInputPatterns(String inputPatterns) {
		setInputPatterns(StringUtils.commaDelimitedListToSet(inputPatterns));
	}

	/**
	 * Sets the input patterns.
	 *
	 * @param inputPatterns the new input patterns
	 */
	public void setInputPatterns(Set<String> inputPatterns) {
		this.inputPatterns = inputPatterns;
	}

	/**
	 * Sets the splitter.
	 *
	 * @param splitter the new splitter
	 */
	public void setSplitter(Splitter splitter) {
		this.splitter = splitter;
	}

	private Set<Resource> resolveResources() throws IOException {
		Set<Resource> resources = new HashSet<Resource>();
		HdfsResourceLoader loader = new HdfsResourceLoader(getConfiguration());
		if (inputPatterns != null) {
			for (String pattern : inputPatterns) {
				resources.addAll(Arrays.asList(loader.getResources(pattern)));
			}
		}
		loader.close();
		return resources;
	}

}
