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
package org.springframework.data.hadoop.cascading.tap.local;

import org.springframework.integration.Message;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.transformer.Transformer;

/**
 * Utility MessageSource using internally a {@link Transformer} and a wrapped {@link MessageSource}. 
 * 
 * @author Costin Leau
 */
class TransformingMessageSource<T> implements MessageSource<T> {

	private final MessageSource<?> source;
	private final Transformer transformer;

	TransformingMessageSource(MessageSource<?> source, Transformer transformer) {
		this.source = source;
		this.transformer = transformer;
	}


	@SuppressWarnings("unchecked")
	@Override
	public Message<T> receive() {
		Message<?> orig = source.receive();
		if (orig != null) {
			return (Message<T>) transformer.transform(orig);
		}
		return null;
	}
}
