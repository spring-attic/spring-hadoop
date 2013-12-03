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
package org.springframework.data.hadoop.store.strategy.naming;

import org.springframework.core.Ordered;
import org.springframework.data.hadoop.store.codec.CodecInfo;

/**
 * Base class for {@code FileNamingStrategy} implementations. This class
 * also implements {@code Ordered} interface to be useful with
 * {@code ChainedFileNamingStrategy} and {@code OrderedComposite}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractFileNamingStrategy implements FileNamingStrategy, Ordered {

	private volatile int order = 0;

	private volatile CodecInfo codecInfo;

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
	public void setCodecInfo(CodecInfo codecInfo) {
		this.codecInfo = codecInfo;
	}

	/**
	 * Sets the order.
	 *
	 * @param order the new order
	 * @see Ordered
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * Gets the codec info.
	 *
	 * @return the codec info
	 */
	public CodecInfo getCodecInfo() {
		return codecInfo;
	}

}
