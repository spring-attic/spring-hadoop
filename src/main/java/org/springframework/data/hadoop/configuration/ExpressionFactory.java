/*
 * Copyright 2006-2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.data.hadoop.configuration;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.springframework.core.MethodParameter;
import org.springframework.data.hadoop.annotation.Key;
import org.springframework.data.hadoop.annotation.Mapper;
import org.springframework.data.hadoop.annotation.Reducer;
import org.springframework.data.hadoop.annotation.Value;
import org.springframework.data.hadoop.annotation.Values;
import org.springframework.data.hadoop.util.reflect.AllParameters;
import org.springframework.data.hadoop.util.reflect.AnnotatedParameters;
import org.springframework.data.hadoop.util.reflect.FirstParameter;
import org.springframework.data.hadoop.util.reflect.MethodUtils;
import org.springframework.data.hadoop.util.reflect.ParameterAction;
import org.springframework.data.hadoop.util.reflect.ParameterMatcher;
import org.springframework.data.hadoop.util.reflect.ParameterRule;
import org.springframework.data.hadoop.util.reflect.ParameterRules;
import org.springframework.data.hadoop.util.reflect.SimpleRule;
import org.springframework.data.hadoop.util.reflect.SingleParameter;
import org.springframework.data.hadoop.util.reflect.TypedParameters;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class ExpressionFactory {

	public String getMapperExpression(Object target) {
		return getMapperExpression(target,
				MethodUtils.getSingleAnnotatedOrPublicMethod(MethodUtils.getTargetClass(target), Mapper.class));
	}

	public String getMapperExpression(Object target, String methodName) {
		return getMapperExpression(target,
				MethodUtils.getSingleNamedMethod(MethodUtils.getTargetClass(target), methodName));
	}

	public String getMapperExpression(Object target, Method method) {
		ExpressionBuilder builder = new ExpressionBuilder(method.getName());
		ParameterRules rules = new ParameterRules(builder.getMapperRules());
		rules.execute(method);
		return builder.build();
	}

	public String getReducerExpression(Object target) {
		return getReducerExpression(target,
				MethodUtils.getSingleAnnotatedOrPublicMethod(MethodUtils.getTargetClass(target), Reducer.class));
	}

	public String getReducerExpression(Object target, String methodName) {
		return getMapperExpression(target,
				MethodUtils.getSingleNamedMethod(MethodUtils.getTargetClass(target), methodName));
	}

	public String getReducerExpression(Object target, Method method) {
		ExpressionBuilder builder = new ExpressionBuilder(method.getName());
		ParameterRules rules = new ParameterRules(builder.getReducerRules());
		rules.execute(method);
		return builder.build();
	}

	private static class ExpressionBuilder {

		private final Map<MethodParameter, String> parameterToExpressionTerm = new HashMap<MethodParameter, String>();

		private final List<MethodParameter> parameters = new ArrayList<MethodParameter>();

		private final String methodName;

		public ExpressionBuilder(String methodName) {
			this.methodName = methodName;

		}

		public String build() {
			List<String> names = new ArrayList<String>();
			for (MethodParameter parameter : parameters) {
				if (parameterToExpressionTerm.containsKey(parameter)) {
					names.add(parameterToExpressionTerm.get(parameter));
				}
			}
			return "#target." + methodName + "(" + StringUtils.collectionToCommaDelimitedString(names) + ")";
		}

		public ParameterRule[] getMapperRules() {
			return new ParameterRule[] { //
			store(), //
					context(), //
					writer(), //
					map(), //
					annotatedValue(), //
					annotatedKey(), //
					onlyParameter("value"), //
					firstParameter("key"), //
					firstParameter("value"), //
					exhausted() //
			};
		}

		public ParameterRule[] getReducerRules() {
			return new ParameterRule[] { //
			store(), //
					context(), //
					writer(), //
					map(), //
					annotatedValues(), //
					annotatedKey(), //
					valuesIterableOrCollection(), //
					firstParameter("key"), //
					firstParameter("values"), //
					exhausted() //
			};
		}

		private ParameterRule store() {
			return new SimpleRule(new ParameterMatcher() {
				public List<MethodParameter> match(List<MethodParameter> parameters) {
					ExpressionBuilder.this.parameters.addAll(parameters);
					return Collections.emptyList();
				}
			}, null);
		}

		private ParameterRule context() {
			return new SimpleRule(new TypedParameters(TaskInputOutputContext.class), new ExpressionToken(
					parameterToExpressionTerm, "context"));
		}

		private ParameterRule writer() {
			return new SimpleRule(new TypedParameters(RecordWriter.class), new ExpressionToken(
					parameterToExpressionTerm, "writer"));
		}

		private ParameterRule map() {
			return new SimpleRule(new TypedParameters(Map.class), new ExpressionToken(parameterToExpressionTerm, "map"));
		}

		private ParameterRule annotatedValue() {
			return new SimpleRule(new AnnotatedParameters(Value.class), new ExpressionToken(parameterToExpressionTerm,
					"value"));
		}

		private ParameterRule annotatedValues() {
			return new SimpleRule(new AnnotatedParameters(Values.class), new ExpressionToken(parameterToExpressionTerm,
					"values"));
		}

		private ParameterRule annotatedKey() {
			return new SimpleRule(new AnnotatedParameters(Key.class), new ExpressionToken(parameterToExpressionTerm,
					"key"));
		}

		private ParameterRule valuesIterableOrCollection() {
			return new SimpleRule(new TypedParameters(Iterable.class), new ExpressionToken(parameterToExpressionTerm,
					"values"));
		}

		private ParameterRule onlyParameter(String name) {
			return new SimpleRule(new SingleParameter(), new ExpressionToken(parameterToExpressionTerm, name));
		}

		private ParameterRule firstParameter(String name) {
			return new SimpleRule(new FirstParameter(), new ExpressionToken(parameterToExpressionTerm, name));
		}

		private ParameterRule exhausted() {
			return new SimpleRule(new AllParameters(), new ParameterAction() {

				public void execute(List<MethodParameter> matches) {
					Assert.state(matches.isEmpty(), "There are remaining parameters after all rules were applied: "
							+ matches);
				}
			});
		}

	}

	private static class ExpressionToken implements ParameterAction {

		private final Map<MethodParameter, String> parameters;

		private final String name;

		public ExpressionToken(Map<MethodParameter, String> parameters, String name) {
			this.parameters = parameters;
			this.name = name;
		}

		public void execute(List<MethodParameter> matches) {
			for (MethodParameter parameter : matches) {
				parameters.put(parameter, name);
			}
		}

	}

}
