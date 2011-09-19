/*
 * Copyright 2006-2011 the original author or authors.
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
package org.springframework.hadoop.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.springframework.hadoop.context.HadoopApplicationContextUtils;

/**
 * @author Dave Syer
 * 
 */
public class AutowiringGroupingComparator<K> implements RawComparator<K>, Configurable {

	private Configuration configuration;

	private RawComparator<K> delegate;

	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		@SuppressWarnings("unchecked")
		RawComparator<K> bean = HadoopApplicationContextUtils.getExistingBean(configuration, RawComparator.class, "groupingComparator");
		delegate = bean;
	}

	public Configuration getConf() {
		return configuration;
	}

	public int compare(K o1, K o2) {
		return delegate.compare(o1, o2);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return delegate.compare(b1, s1, l1, b2, s2, l2);
	}

}
