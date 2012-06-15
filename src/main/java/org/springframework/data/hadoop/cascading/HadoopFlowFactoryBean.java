/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.cascading;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.util.StringUtils;

import cascading.cascade.Cascade;
import cascading.flow.FlowDef;
import cascading.flow.FlowProps;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;


/**
 * Factory for declarative {@link HadoopFlow} creation. Usually used with a {@link Cascade}. 
 * 
 * Note the flow is not started.
 * 
 * @author Costin Leau
 */
public class HadoopFlowFactoryBean extends FlowFactoryBean<HadoopFlow> implements BeanNameAware {

	private static String MARKER = HadoopFlowFactoryBean.class.getName() + "#SINGLE";

	private Configuration configuration;
	private Properties properties;

	private String beanName;

	private Integer maxConcurrentSteps;
	private Long jobPoolingInterval;

	private Map<String, Tap> sources;
	private Map<String, Tap> sinks;
	private Map<String, Tap> traps;
	private Collection<Pipe> tails;

	private FlowDef flowDef;

	@Override
	HadoopFlow createFlow() {
		// copy flowDef
		FlowDef def = FlowDef.flowDef();

		if (flowDef != null) {
			def.addSinks(flowDef.getSinksCopy()).addSources(flowDef.getSourcesCopy()).addTraps(flowDef.getTrapsCopy()).addTails(
					flowDef.getTailsArray()).setAssertionLevel(flowDef.getAssertionLevel()).setDebugLevel(
					flowDef.getDebugLevel()).addCheckpoints(flowDef.getCheckpointsCopy()).addTags(flowDef.getTags()).setName(
					flowDef.getName());
		}

		Set<Pipe> heads = new LinkedHashSet<Pipe>();

		if (tails != null) {
			for (Pipe pipe : tails) {
				Collections.addAll(heads, pipe.getHeads());
			}
		}

		Pipe pipe = null;

		if (heads.size() == 1) {
			pipe = heads.iterator().next();
		}

		if (sources != null && sources.size() == 1) {
			Tap tap = sources.remove(MARKER);
			if (tap != null) {
				sources.put(pipe.getName(), tap);
			}
		}

		if (sinks != null && sinks.size() == 1) {
			Tap tap = sinks.remove(MARKER);
			if (tap != null) {
				sinks.put(pipe.getName(), tap);
			}
		}

		def.addSources(sources).addSinks(sinks).addTraps(traps);

		if (tails != null) {
			def.addTails(tails);
		}

		if (StringUtils.hasText(beanName)) {
			def.addTag(beanName);

			if (!StringUtils.hasText(def.getName())) {
				def.setName(beanName);
			}
		}


		Properties props = ConfigurationUtils.asProperties(ConfigurationUtils.createFrom(configuration, properties));

		if (jobPoolingInterval != null) {
			FlowProps.setJobPollingInterval(props, jobPoolingInterval);
		}

		if (maxConcurrentSteps != null) {
			FlowProps.setMaxConcurrentSteps(props, maxConcurrentSteps);
		}

		HadoopFlow flow = (HadoopFlow) new HadoopFlowConnector(props).connect(def);

		return flow;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}


	/**
	 * Sets the configuration.
	 *
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Sets the properties.
	 *
	 * @param properties The properties to set.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Sets the max concurrent steps.
	 *
	 * @param maxConcurrentSteps The maxConcurrentSteps to set.
	 */
	public void setMaxConcurrentSteps(Integer maxConcurrentSteps) {
		this.maxConcurrentSteps = maxConcurrentSteps;
	}

	/**
	 * Sets the job pooling interval.
	 *
	 * @param jobPoolingInterval The jobPoolingInterval to set.
	 */
	public void setJobPoolingInterval(Long jobPoolingInterval) {
		this.jobPoolingInterval = jobPoolingInterval;
	}


	/**
	 * Sets the sources.
	 *
	 * @param sources The sources to set.
	 */
	public void setSources(Map<String, Tap> sources) {
		this.sources = sources;
	}

	public void setSource(Tap source) {
		Map<String, Tap> sources = new HashMap<String, Tap>();
		sources.put(MARKER, source);
		this.sources = sources;
	}

	/**
	 * Sets the sinks.
	 *
	 * @param sinks The sinks to set.
	 */
	public void setSinks(Map<String, Tap> sinks) {
		this.sinks = sinks;
	}

	public void setSink(Tap sink) {
		Map<String, Tap> sinks = new HashMap<String, Tap>();
		sinks.put(MARKER, sink);
		this.sinks = sinks;
	}

	/**
	 * Sets the traps.
	 *
	 * @param traps The traps to set.
	 */
	public void setTraps(Map<String, Tap> traps) {
		this.traps = traps;
	}

	/**
	 * Sets the tails.
	 *
	 * @param tails The tails to set.
	 */
	public void setTails(Collection<Pipe> tails) {
		this.tails = tails;
	}

	/**
	 * Sets the tail.
	 *
	 * @param tail the new tail
	 */
	public void setTail(Pipe tail) {
		this.tails = new ArrayList<Pipe>(1);
		tails.add(tail);
	}

	/**
	 * Sets the flow definition. Useful for programatically setting up the flow
	 * and then letting Spring do the configuration wiring.
	 * 
	 * @param flowDef
	 */
	public void setFlowDef(FlowDef flowDef) {
		this.flowDef = flowDef;
	}
}