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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Spring {@link Converter} which knows how to convert
 * {@link MindRpcMessageHolder} to {@link BaseObject}.
 *
 * @author Janne Valkealahti
 *
 */
public class MindHolderToObjectConverter implements Converter<MindRpcMessageHolder, BaseObject> {

	private final static Log log = LogFactory.getLog(MindHolderToObjectConverter.class);

	/** Jackson object mapper */
	private ObjectMapper objectMapper;

	/** Base package prefixes */
	private String[] basePackage;

	/** Simple class cache*/
	private Map<String, Class<? extends BaseObject>> classCache;

	/**
	 * Instantiates a new mind holder to object converter.
	 *
	 * @param objectMapper the object mapper
	 */
	public MindHolderToObjectConverter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
		this.classCache = new ConcurrentHashMap<String, Class<? extends BaseObject>>();
	}

	/**
	 * Instantiates a new mind holder to object converter.
	 *
	 * @param objectMapper the object mapper
	 * @param basePackage the base package
	 */
	public MindHolderToObjectConverter(ObjectMapper objectMapper, String basePackage) {
		this(objectMapper, new String[]{basePackage});
	}

	/**
	 * Instantiates a new mind holder to object converter.
	 *
	 * @param objectMapper the object mapper
	 * @param basePackage the array of base packages
	 */
	public MindHolderToObjectConverter(ObjectMapper objectMapper, String basePackage[]) {
		this.objectMapper = objectMapper;
		this.basePackage = basePackage;
		this.classCache = new ConcurrentHashMap<String, Class<? extends BaseObject>>();
	}

	@Override
	public BaseObject convert(MindRpcMessageHolder source) {
		Map<String, String> headers = source.getHeaders();
		String type = headers.get("type").trim();
		byte[] content = source.getContent();
		Class<?> clazz = resolveClass(type);

		if (clazz == null) {
			throw new MindDataConversionException("Failed to convert source object. Can't resolve type=" + type);
		}

		try {
			BaseObject object = (BaseObject) objectMapper.readValue(content, clazz);
			return object;
		} catch (Exception e) {
			throw new MindDataConversionException("Failed to convert source object.", e);
		}
	}

	/**
	 * Sets the base packages.
	 *
	 * @param basePackage the new array base packages
	 */
	public void setBasePackage(String[] basePackage) {
		this.basePackage = basePackage;
	}

	/**
	 * Resolve the class.
	 *
	 * @param type the type
	 * @return the class
	 */
	@SuppressWarnings("unchecked")
	private Class<?> resolveClass(String type) {
		String clazzName = type;
		Class<?> clazz = classCache.get(clazzName);

		if (clazz == null) {
			try {
				clazz = ClassUtils.resolveClassName(clazzName, getClass().getClassLoader());
			} catch (IllegalArgumentException e) {
			}
			if (clazz != null) {
				classCache.put(clazzName, (Class<? extends BaseObject>) clazz);
			}
		}
		if (clazz != null) {
			return clazz;
		}

		if (!ObjectUtils.isEmpty(basePackage)) {
			for (String base : basePackage) {
				clazzName = base + "." + type;
				if (log.isDebugEnabled()) {
					log.debug("Trying to resolve type " + type + " as " + clazzName);
				}
				clazz = classCache.get(clazzName);
				if (log.isDebugEnabled() && clazz != null) {
					log.debug("Found " + clazz + " from a cache");
				}
				if (clazz == null) {
					try {
						clazz = ClassUtils.resolveClassName(clazzName, getClass().getClassLoader());
					} catch (IllegalArgumentException e) {
					}
					if (clazz != null) {
						classCache.put(clazzName, (Class<? extends BaseObject>) clazz);
					}
				}
				if (clazz != null) {
					return clazz;
				}
			}
		}

		return clazz;
	}

}
