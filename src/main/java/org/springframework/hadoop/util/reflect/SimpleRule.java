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
package org.springframework.hadoop.util.reflect;

import java.util.Collections;
import java.util.List;

import org.springframework.core.MethodParameter;

/**
 * @author Dave Syer
 * 
 */
public class SimpleRule implements ParameterRule {

	private final ParameterMatcher matcher;

	private final ParameterAction action;

	public SimpleRule(ParameterMatcher matcher, ParameterAction action) {
		this.matcher = matcher;
		this.action = action;
	}

	public List<MethodParameter> match(List<MethodParameter> parameters) {
		return matcher == null ? Collections.<MethodParameter> emptyList() : matcher.match(parameters);
	}

	public void execute(List<MethodParameter> matches) {
		if (action != null) {
			action.execute(matches);
		}
	}

}
