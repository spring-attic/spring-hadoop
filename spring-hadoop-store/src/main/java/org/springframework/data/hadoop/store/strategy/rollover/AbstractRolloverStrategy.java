/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store.strategy.rollover;

import org.springframework.core.Ordered;


/**
 * Base class for {@code RolloverStrategy} implementations. This class also implements {@code Ordered} interface to be
 * useful with {@code ChainedFileRolloverStrategy} and {@code OrderedComposite}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractRolloverStrategy implements RolloverStrategy, Ordered {

	private volatile int order = 0;

	private volatile long position;

	/**
	 * Implementation should override this method to define a chaining order.
	 *
	 * @return the order value
	 * @see Ordered#getOrder()
	 */
	@Override
	public int getOrder() {
		return order;
	}

	@Override
	public void setWritePosition(long position) {
		this.position = position;
	}

	@Override
	public abstract RolloverStrategy createInstance();

	/**
	 * Sets the order.
	 *
	 * @param order the new order
	 * @see Ordered
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	public long getPosition() {
		return position;
	}

}
