/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.test.support;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.yarn.test.context.YarnCluster;

/**
 * Simple factory bean delegating retrieval of 
 * hadoop {@link Configuration} from a {@link StandaloneYarnCluster}.
 * 
 * @author Janne Valkealahti
 *
 */
public class ConfigurationDelegatingFactoryBean implements InitializingBean, FactoryBean<Configuration> {

	private YarnCluster cluster;
	
	@Override
	public Configuration getObject() throws Exception {
		return cluster.getConfiguration();
	}

	@Override
	public Class<Configuration> getObjectType() {
		return Configuration.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(cluster, "StandaloneYarnCluster must be set");
	}

	/**
	 * Sets the {@link StandaloneYarnCluster}
	 * 
	 * @param cluster the {@link StandaloneYarnCluster}
	 */
	public void setCluster(YarnCluster cluster) {
		this.cluster = cluster;
	}

}
