/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.integration.convert;

import java.util.HashMap;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Spring {@link Converter} which knows how to convert
 * {@link BaseObject} to {@link MindRpcMessageHolder}.
 *
 * @author Janne Valkealahti
 *
 */
public class MindObjectToHolderConverter implements Converter<BaseObject, MindRpcMessageHolder> {

	/** Jackson object mapper */
	private ObjectMapper objectMapper;

	/**
	 * Constructs converter with a jackson object mapper.
	 *
	 * @param objectMapper the object mapper
	 */
	public MindObjectToHolderConverter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public MindRpcMessageHolder convert(BaseObject source) {
		try {
			String type = source.getType();
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("type", type);
			byte[] content = objectMapper.writeValueAsBytes(source);
			return new MindRpcMessageHolder(headers, content);
		} catch (JsonProcessingException e) {
			throw new MindDataConversionException("Failed to convert source object.", e);
		}
	}

}
