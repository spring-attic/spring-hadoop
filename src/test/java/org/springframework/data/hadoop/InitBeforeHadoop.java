/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.BeanNameAware;

/**
 * @author Costin Leau
 */
public class InitBeforeHadoop implements BeanNameAware {

	public static Map<String, Boolean> INSTANCES = new LinkedHashMap<String, Boolean>();

	public Boolean initialized = Boolean.FALSE;
	private String name;

	@PostConstruct
	void init() {
		initialized = Boolean.TRUE;
		INSTANCES.put(name, initialized);
	}

	@PreDestroy
	void destroy() {
		INSTANCES.remove(name);
	}

	@Override
	public void setBeanName(String name) {
		this.name = name;
	}
}
