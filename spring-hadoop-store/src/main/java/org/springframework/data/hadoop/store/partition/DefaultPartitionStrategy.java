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
package org.springframework.data.hadoop.store.partition;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.expression.MapExpressionMethods;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A {@link PartitionStrategy} which is used to provide a generic partitioning support using Spring SpEL.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultPartitionStrategy<T extends Object> extends AbstractPartitionStrategy<T,Map<String,Object>> {

	private final static Log log = LogFactory.getLog(DefaultPartitionStrategy.class);

	/**
	 * Instantiates a new message partition strategy.
	 *
	 * @param expression the expression
	 */
	public DefaultPartitionStrategy(Expression expression) {
		super(new MapPartitionResolver(expression), new MapPartitionKeyResolver<T>());
	}

	/**
	 * Instantiates a new message partition strategy.
	 *
	 * @param expression the expression
	 */
	public DefaultPartitionStrategy(String expression) {
		super(new MapPartitionResolver(expression), new MapPartitionKeyResolver<T>());
	}

	/**
	 * A {@link PartitionResolver} which uses an {@link Expression} together with
	 * {@link MessageExpressionMethods} to evaluate new {@link Path}s.
	 */
	private static class MapPartitionResolver implements PartitionResolver<Map<String,Object>> {

		private final Expression expression;
		private final MapExpressionMethods methods;

		public MapPartitionResolver(String expression) {
			ExpressionParser parser = new SpelExpressionParser();
			this.expression = parser.parseExpression(expression);
			this.methods = new MapExpressionMethods();
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		public MapPartitionResolver(Expression expression) {
			this.expression = expression;
			this.methods = new MapExpressionMethods();
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		@Override
		public Path resolvePath(Map<String,Object> partitionKey) {
			return new Path(methods.getValue(expression, partitionKey, String.class));
		}

	}

	/**
	 * A {@link PartitionKeyResolver} which simply creates a new {@link Map}
	 * as a partition key using an passed in entity.
	 */
	private static class MapPartitionKeyResolver<T extends Object> implements PartitionKeyResolver<T,Map<String,Object>> {

		@Override
		public Map<String,Object> resolvePartitionKey(T entity) {
			return new DefaultPartitionKey(entity);
		}

	}

}
