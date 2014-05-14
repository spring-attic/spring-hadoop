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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.expression.MessageExpressionMethods;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * A {@link PartitionStrategy} which is used to provide a generic partitioning support using Spring SpEL.
 *
 * @author Janne Valkealahti
 *
 */
public class MessagePartitionStrategy<T extends Object> extends AbstractPartitionStrategy<T,Message<?>> {

	private final static Log log = LogFactory.getLog(MessagePartitionStrategy.class);

	/**
	 * Instantiates a new message partition strategy.
	 *
	 * @param expression the expression
	 */
	public MessagePartitionStrategy(Expression expression) {
		super(new MessagePartitionResolver(expression), new MessagePartitionKeyResolver<T>());
	}

	/**
	 * Instantiates a new message partition strategy.
	 *
	 * @param expression the expression
	 */
	public MessagePartitionStrategy(String expression) {
		super(new MessagePartitionResolver(expression), new MessagePartitionKeyResolver<T>());
	}

	/**
	 * A {@link PartitionResolver} which uses an {@link Expression} together with
	 * {@link MessageExpressionMethods} to evaluate new {@link Path}s.
	 */
	public static class MessagePartitionResolver implements PartitionResolver<Message<?>> {

		private final Expression expression;
		private final MessageExpressionMethods methods;

		public MessagePartitionResolver(String expression) {
			ExpressionParser parser = new SpelExpressionParser();
			this.expression = parser.parseExpression(expression);
			this.methods = new MessageExpressionMethods();
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		public MessagePartitionResolver(Expression expression) {
			this.expression = expression;
			this.methods = new MessageExpressionMethods();
			log.info("Using expression=[" + this.expression.getExpressionString() + "]");
		}

		@Override
		public Path resolvePath(Message<?> partitionKey) {
			return new Path(methods.getValue(expression, partitionKey, String.class));
		}

	}

	/**
	 * A {@link PartitionKeyResolver} which simply creates a new {@link Message}
	 * as a partition key using an passed in entity.
	 */
	public static class MessagePartitionKeyResolver<T extends Object> implements PartitionKeyResolver<T,Message<?>> {

		@Override
		public Message<?> resolvePartitionKey(T entity) {
			return MessageBuilder.withPayload(entity).build();
		}

	}

}
