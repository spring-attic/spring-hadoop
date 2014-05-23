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
package org.springframework.data.hadoop.store.strategy.naming;

import org.apache.hadoop.fs.Path;
import org.springframework.core.Ordered;
import org.springframework.data.hadoop.store.codec.CodecInfo;

/**
 * Base class for {@code FileNamingStrategy} implementations.
 *
 * <p>Also implements {@code Ordered} interface to be useful with
 * {@code ChainedFileNamingStrategy} and {@code OrderedComposite}.
 *
 * <p>Enable information from method {@link #isEnabled()} can be used in cases
 * where strategy needs to be in place but perhaps should not be activated.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractFileNamingStrategy implements FileNamingStrategy, Ordered {

	private volatile int order = 0;

	private volatile CodecInfo codecInfo;

	private volatile boolean enabled = true;

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

	@Override
	public Path init(Path path) {
		return path;
	}

	@Override
	public void reset() {
	}

	@Override
	public abstract FileNamingStrategy createInstance();

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

	/**
	 * Sets if this strategy is enabled.
	 *
	 * @param enabled the new enabled
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	/**
	 * Checks if strategy is enabled.
	 *
	 * @return true, if is enabled
	 */
	public boolean isEnabled() {
		return enabled;
	}

}
