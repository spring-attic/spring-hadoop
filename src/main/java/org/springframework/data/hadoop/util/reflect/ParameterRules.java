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
package org.springframework.data.hadoop.util.reflect;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.core.MethodParameter;
import org.springframework.core.ParameterNameDiscoverer;

/**
 * @author Dave Syer
 * 
 */
public class ParameterRules {

	private final ParameterRule[] rules;

	private final ParameterNameDiscoverer parameterNameDiscoverer;

	public ParameterRules(ParameterNameDiscoverer parameterNameDiscoverer, ParameterRule... rules) {
		this.parameterNameDiscoverer = parameterNameDiscoverer;
		this.rules = rules;
	}

	public ParameterRules(ParameterRule... rules) {
		this(new LocalVariableTableParameterNameDiscoverer(), rules);
	}

	public List<MethodParameter> execute(Method method) {
		List<MethodParameter> parameters = MethodUtils.getParameterTypes(method, parameterNameDiscoverer);
		for (ParameterRule rule : rules) {
			List<MethodParameter> next = new ArrayList<MethodParameter>(parameters);
			List<MethodParameter> matches = rule.match(parameters);
			if (!matches.isEmpty()) {
				for (MethodParameter parameter : matches) {
					next.remove(parameter);
				}
				rule.execute(matches);
			}
			parameters = next;
		}
		return parameters;
	}

}
