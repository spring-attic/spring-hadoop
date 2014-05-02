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
package org.springframework.yarn.integration.support;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A FactoryBean for creating a Jackson {@link ObjectMapper} with setters to
 * enable or disable Jackson features from within XML configuration.
 *
 * <p>In case there are no specific setters provided (for some rarely used
 * options), you can still use the more general methods
 * {@link #setFeaturesToEnable(Object[])} and {@link #setFeaturesToDisable(Object[])}.
 *
 * <pre>
 * &lt;bean class="org.springframework.yarn.integration.support.Jackson2ObjectMapperFactoryBean"&gt;
 *   &lt;property name="featuresToEnable"&gt;
 *     &lt;array&gt;
 *       &lt;util:constant static-field="com.fasterxml.jackson.databind.SerializationFeature$WRAP_ROOT_VALUE"/&gt;
 *       &lt;util:constant static-field="com.fasterxml.jackson.databind.SerializationFeature$CLOSE_CLOSEABLE"/&gt;
 *     &lt;/array&gt;
 *   &lt;/property&gt;
 *   &lt;property name="featuresToDisable"&gt;
 *     &lt;array&gt;
 *       &lt;util:constant static-field="com.fasterxml.jackson.databind.MapperFeature$USE_ANNOTATIONS"/&gt;
 *     &lt;/array&gt;
 *   &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 *
 * <p>Note: This BeanFctory is singleton, so if you need more than one you'll need
 * to configure multiple instances.
 *
 * @author <a href="mailto:dmitry.katsubo@gmail.com">Dmitry Katsubo</a>
 * @author Rossen Stoyanchev
 * @author Janne Valkealahti
 *
 */
public class Jackson2ObjectMapperFactoryBean implements FactoryBean<ObjectMapper>, InitializingBean {

	private ObjectMapper objectMapper;

	private Map<Object, Boolean> features = new HashMap<Object, Boolean>();

	private DateFormat dateFormat;

	private AnnotationIntrospector annotationIntrospector;

	private final Map<Class<?>, JsonSerializer<?>> serializers = new LinkedHashMap<Class<?>, JsonSerializer<?>>();

	private final Map<Class<?>, JsonDeserializer<?>> deserializers = new LinkedHashMap<Class<?>, JsonDeserializer<?>>();


	/**
	 * Set the ObjectMapper instance to use. If not set, the ObjectMapper will
	 * be created using its default constructor.
	 *
	 * @param objectMapper object mapper
	 */
	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	/**
	 * Define the format for date/time with the given {@link DateFormat}.
	 * @see #setSimpleDateFormat(String)
	 *
	 * @param dateFormat date format
	 */
	public void setDateFormat(DateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}

	/**
	 * Define the date/time format with a {@link SimpleDateFormat}.
	 * @see #setDateFormat(DateFormat)
	 *
	 * @param format format
	 */
	public void setSimpleDateFormat(String format) {
		this.dateFormat = new SimpleDateFormat(format);
	}

	/**
	 * Set the {@link AnnotationIntrospector} for both serialization and
	 * deserialization.
	 *
	 * @param annotationIntrospector the AnnotationIntrospector
	 */
	public void setAnnotationIntrospector(AnnotationIntrospector annotationIntrospector) {
		this.annotationIntrospector = annotationIntrospector;
	}

	/**
	 * Configure custom serializers. Each serializer is registered for the type
	 * returned by {@link JsonSerializer#handledType()}, which must not be
	 * {@code null}.
	 * @see #setSerializersByType(Map)
	 *
	 * @param serializers serializers
	 */
	public void setSerializers(JsonSerializer<?>... serializers) {
		if (serializers != null) {
			for (JsonSerializer<?> serializer : serializers) {
				Class<?> handledType = serializer.handledType();
				Assert.isTrue(handledType != null && handledType != Object.class,
						"Unknown handled type in " + serializer.getClass().getName());
				this.serializers.put(serializer.handledType(), serializer);
			}
		}
	}

	/**
	 * Configure custom serializers for the given types.
	 * @see #setSerializers(JsonSerializer...)
	 *
	 * @param serializers serializers
	 */
	public void setSerializersByType(Map<Class<?>, JsonSerializer<?>> serializers) {
		if (serializers != null) {
			this.serializers.putAll(serializers);
		}
	}

	/**
	 * Configure custom deserializers for the given types.
	 *
	 * @param deserializers deserializers
	 */
	public void setDeserializersByType(Map<Class<?>, JsonDeserializer<?>> deserializers) {
		if (deserializers != null) {
			this.deserializers.putAll(deserializers);
		}
	}

	/**
	 * Shortcut for {@link MapperFeature#AUTO_DETECT_FIELDS} option.
	 *
	 * @param autoDetectFields autoDetectFields
	 */
	public void setAutoDetectFields(boolean autoDetectFields) {
		this.features.put(MapperFeature.AUTO_DETECT_FIELDS, autoDetectFields);
	}

	/**
	 * Shortcut for {@link MapperFeature#AUTO_DETECT_SETTERS}/
	 * {@link MapperFeature#AUTO_DETECT_GETTERS} option.
	 *
	 * @param autoDetectGettersSetters autoDetectGettersSetters
	 */
	public void setAutoDetectGettersSetters(boolean autoDetectGettersSetters) {
		this.features.put(MapperFeature.AUTO_DETECT_SETTERS, autoDetectGettersSetters);
		this.features.put(MapperFeature.AUTO_DETECT_GETTERS, autoDetectGettersSetters);
	}

	/**
	 * Shortcut for {@link SerializationFeature#FAIL_ON_EMPTY_BEANS} option.
	 *
	 * @param failOnEmptyBeans failOnEmptyBeans
	 */
	public void setFailOnEmptyBeans(boolean failOnEmptyBeans) {
		this.features.put(SerializationFeature.FAIL_ON_EMPTY_BEANS, failOnEmptyBeans);
	}

	/**
	 * Shortcut for {@link SerializationFeature#INDENT_OUTPUT} option.
	 *
	 * @param indentOutput indentOutput
	 */
	public void setIndentOutput(boolean indentOutput) {
		this.features.put(SerializationFeature.INDENT_OUTPUT, indentOutput);
	}

	/**
	 * Specify features to enable.
	 *
	 * @see com.fasterxml.jackson.databind.MapperFeature
	 * @see com.fasterxml.jackson.databind.SerializationFeature
	 * @see com.fasterxml.jackson.databind.DeserializationFeature
	 * @see com.fasterxml.jackson.core.JsonParser.Feature
	 * @see com.fasterxml.jackson.core.JsonGenerator.Feature
	 *
	 * @param featuresToEnable featuresToEnable
	 */
	public void setFeaturesToEnable(Object... featuresToEnable) {
		if (featuresToEnable != null) {
			for (Object feature : featuresToEnable) {
				this.features.put(feature, Boolean.TRUE);
			}
		}
	}

	/**
	 * Specify features to disable.
	 *
	 * @see com.fasterxml.jackson.databind.MapperFeature
	 * @see com.fasterxml.jackson.databind.SerializationFeature
	 * @see com.fasterxml.jackson.databind.DeserializationFeature
	 * @see com.fasterxml.jackson.core.JsonParser.Feature
	 * @see com.fasterxml.jackson.core.JsonGenerator.Feature
	 *
	 * @param featuresToDisable featuresToDisable
	 */
	public void setFeaturesToDisable(Object... featuresToDisable) {
		if (featuresToDisable != null) {
			for (Object feature : featuresToDisable) {
				this.features.put(feature, Boolean.FALSE);
			}
		}
	}

	public void afterPropertiesSet() throws FatalBeanException {
		if (this.objectMapper == null) {
			this.objectMapper = new ObjectMapper();
		}

		if (this.dateFormat != null) {
			this.objectMapper.setDateFormat(this.dateFormat);
		}

		if (this.serializers != null || this.deserializers != null) {
			SimpleModule module = new SimpleModule();
			addSerializers(module);
			addDeserializers(module);
			this.objectMapper.registerModule(module);
		}

		if (this.annotationIntrospector != null) {
			this.objectMapper.setAnnotationIntrospector(this.annotationIntrospector);
		}

		for (Object feature : this.features.keySet()) {
			configureFeature(feature, this.features.get(feature));
		}
	}

	@SuppressWarnings("unchecked")
	private <T> void addSerializers(SimpleModule module) {
		for (Class<?> type : this.serializers.keySet()) {
			module.addSerializer((Class<? extends T>) type, (JsonSerializer<T>) this.serializers.get(type));
		}
	}

	@SuppressWarnings("unchecked")
	private <T> void addDeserializers(SimpleModule module) {
		for (Class<?> type : this.deserializers.keySet()) {
			module.addDeserializer((Class<T>) type, (JsonDeserializer<? extends T>) this.deserializers.get(type));
		}
	}

	private void configureFeature(Object feature, boolean enabled) {
		if (feature instanceof MapperFeature) {
			this.objectMapper.configure((MapperFeature) feature, enabled);
		}
		else if (feature instanceof DeserializationFeature) {
			this.objectMapper.configure((DeserializationFeature) feature, enabled);
		}
		else if (feature instanceof SerializationFeature) {
			this.objectMapper.configure((SerializationFeature) feature, enabled);
		}
		else if (feature instanceof JsonParser.Feature) {
			this.objectMapper.configure((JsonParser.Feature) feature, enabled);
		}
		else if (feature instanceof JsonGenerator.Feature) {
			this.objectMapper.configure((JsonGenerator.Feature) feature, enabled);
		}
		else {
			throw new FatalBeanException("Unknown feature class " + feature.getClass().getName());
		}
	}

	/**
	 * Return the singleton ObjectMapper.
	 *
	 * @return object mapper
	 */
	public ObjectMapper getObject() {
		return this.objectMapper;
	}

	public Class<?> getObjectType() {
		return ObjectMapper.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
