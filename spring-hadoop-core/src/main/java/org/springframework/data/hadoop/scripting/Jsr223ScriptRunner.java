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
package org.springframework.data.hadoop.scripting;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scripting.ScriptSource;

/**
 * Runner for easy declarative use of JSR-223 based {@link ScriptEvaluator}.
 * 
 * @author Costin Leau
 */
class Jsr223ScriptRunner implements InitializingBean, BeanClassLoaderAware, Callable<Object> {

	private ClassLoader classLoader;
	private ScriptSource script;
	private Jsr223ScriptEvaluator evaluator;
	private String language, extension;
	private Map<String, Object> arguments;
	private EvaluationPolicy evaluation = EvaluationPolicy.ALWAYS;
	private final Object monitor = new Object();
	private volatile boolean evaluated;
	private Object result = null;
	private boolean runAtStartup = false;

	private Iterable<Callable<?>> preActions;
	private Iterable<Callable<?>> postActions;


	@Override
	public Object call() throws Exception {
		invoke(preActions);

		Object res;

		switch (evaluation) {
		case ONCE:
			if (!evaluated) {
				synchronized (monitor) {
					if (!evaluated) {
						evaluated = true;
						result = evaluator.evaluate(script, arguments);
					}
				}
			}
			res = result;
			break;
		case IF_MODIFIED:
			// isModified is synchronized so only one thread will see the update
			if (script.isModified()) {
				result = evaluator.evaluate(script, arguments);
			}
			res = result;
			break;
		default:
			res = evaluator.evaluate(script, arguments);
		}

		invoke(postActions);
		return res;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		evaluator = new Jsr223ScriptEvaluator(classLoader);
		evaluator.setLanguage(language);
		evaluator.setExtension(extension);

		if (arguments == null) {
			arguments = new LinkedHashMap<String, Object>();
		}
		postProcess(arguments);

		if (runAtStartup) {
			call();
		}
	}

	/**
	 * Method for post-processing arguments. Useful for enhancing (adding) new arguments to scripts
	 * being executed.
	 * 
	 * @param arguments The arguments.
	 */
	protected void postProcess(Map<String, Object> arguments) {
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Sets the script source to evaluate.
	 * 
	 * @param script The script to evaluate.
	 */
	public void setScriptSource(ScriptSource script) {
		this.script = script;
	}

	/**
	 * Sets the script language. 
	 * 
	 * @param language The script language.
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * Sets the script extension. Used for detecting the language.
	 * 
	 * @param extension The extension to set.
	 */
	public void setExtension(String extension) {
		this.extension = extension;
	}

	/**
	 * Sets the way the script is evaluated.
	 * 
	 * @param evaluation The evaluation.
	 */
	public void setEvaluate(EvaluationPolicy evaluation) {
		this.evaluation = evaluation;
	}

	/**
	 * Sets the arguments for evaluating this script.
	 * 
	 * @param arguments The arguments to set.
	 */
	public void setArguments(Map<String, Object> arguments) {
		this.arguments = arguments;
	}

	/**
	 * Indicates whether the script gets executed once the factory bean initializes.
	 * 
	 * @return true if the script runs or not during startup
	 */
	public boolean isRunAtStartup() {
		return runAtStartup;
	}

	/**
	 * Indicates whether to evaluate the script at startup (default) or not.
	 *
	 * @param runAtStartup The runStartUp to set.
	 */
	public void setRunAtStartup(boolean runAtStartup) {
		this.runAtStartup = runAtStartup;
	}


	/**
	 * Actions to be invoked before running the action.
	 * 
	 * @param actions The actions.
	 */
	public void setPreAction(Collection<Callable<?>> actions) {
		this.preActions = actions;
	}

	/**
	 * Actions to be invoked after running the action.
	 * 
	 * @param actions The actions.
	 */
	public void setPostAction(Collection<Callable<?>> actions) {
		this.postActions = actions;
	}

	private void invoke(Iterable<Callable<?>> actions) throws Exception {
		if (actions != null) {
			for (Callable<?> action : actions) {
				action.call();
			}
		}
	}
}