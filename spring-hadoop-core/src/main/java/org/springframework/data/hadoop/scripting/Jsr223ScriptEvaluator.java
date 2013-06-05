/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop.scripting;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scripting.ScriptCompilationException;
import org.springframework.scripting.ScriptSource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Jsr233/javax.scripting implementation of {@link ScriptEvaluator}. 
 * 
 * @author Costin Leau
 */
class Jsr223ScriptEvaluator implements ScriptEvaluator {

	private final Log log = LogFactory.getLog(getClass());

	private String language;
	private String extension;
	private ClassLoader classLoader;


	/**
	 * Constructs a new <code>Jsr223ScriptEvaluator</code> instance.
	 */
	public Jsr223ScriptEvaluator() {
		this(null);
	};

	/**
	 * Constructs a new <code>Jsr223ScriptEvaluator</code> instance.
	 *
	 * @param classLoader class loader to use
	 */
	public Jsr223ScriptEvaluator(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public Object evaluate(ScriptSource script) {
		return evaluate(script, Collections.<String, Object> emptyMap());
	}

	@Override
	public Object evaluate(ScriptSource script, Map<String, Object> arguments) {
		ScriptEngine engine = discoverEngine(script, arguments);

		Bindings bindings = (!CollectionUtils.isEmpty(arguments) ? new SimpleBindings(arguments) : null);

		try {
			return (bindings == null ? engine.eval(script.getScriptAsString()) : engine.eval(
					script.getScriptAsString(), bindings));
		} catch (IOException ex) {
			throw new ScriptCompilationException(script, "Cannot access script", ex);
		} catch (ScriptException ex) {
			throw new ScriptCompilationException(script, "Execution failure", ex);
		}
	}

	protected ScriptEngine discoverEngine(ScriptSource script, Map<String, Object> arguments) {
		ScriptEngineManager engineManager = new ScriptEngineManager(classLoader);
		ScriptEngine engine = null;

		if (StringUtils.hasText(language)) {
			engine = engineManager.getEngineByName(language);
		}
		else {
			// make use the extension (enhanced ScriptSource interface)
			Assert.hasText(extension, "no language or extension specified");
			engine = engineManager.getEngineByExtension(extension);
		}

		Assert.notNull(engine, "No suitable engine found for "
				+ (StringUtils.hasText(language) ? "language " + language : "extension " + extension));

		if (log.isDebugEnabled()) {
			ScriptEngineFactory factory = engine.getFactory();
			log.debug(String.format("Using ScriptEngine %s (%s), language %s (%s)", factory.getEngineName(),
					factory.getEngineVersion(), factory.getLanguageName(), factory.getLanguageVersion()));
		}

		return engine;
	}

	/**
	 * Sets the extension of the language meant for evaluation the scripts.. 
	 * 
	 * @param extension The extension to set.
	 */
	public void setExtension(String extension) {
		this.extension = extension;
	}

	/**
	 * Sets the name of language meant for evaluation the scripts.
	 * 
	 * @param language The language to set.
	 */
	public void setLanguage(String language) {
		this.language = language;
	}
}