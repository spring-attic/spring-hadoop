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
package org.springframework.yarn.launch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base implementation used for launching a Spring Application
 * Context and executing a bean using a command line. This
 * command line runner is meant to be used from a subclass.
 * <p>
 * The general idea of this launcher concept is to provide
 * a way to define context config location, bean name for execution
 * handling, options and a arguments. Possible examples are:
 * <br>
 * <pre>
 * contextConfig
 * contextConfig,childContextConfig beanIdentifier
 * contextConfig beanIdentifier &lt;arguments&gt;
 * contextConfig &lt;options&gt; beanIdentifier
 * contextConfig &lt;options&gt; beanIdentifier &lt;arguments&gt;
 * &lt;options&gt; contextConfig &lt;options&gt; beanIdentifier &lt;arguments&gt;
 * </pre>
 *
 * @author Janne Valkealahti
 *
 * @param <T> the type of bean to run
 */
public abstract class AbstractCommandLineRunner<T> {

	private static final Log log = LogFactory.getLog(AbstractCommandLineRunner.class);

	/** Mapper for exit codes */
	private ExitCodeMapper exitCodeMapper = new SimpleJvmExitCodeMapper();

	/** Static error message holder for testing */
	private static String message = "";

	/** Exiter helping for testing */
	private static SystemExiter systemExiter = new JvmSystemExiter();

	/**
	 * Gets the static error message set for
	 * this class. This is useful for tests.
	 *
	 * @return the static error message
	 */
	public static String getErrorMessage() {
		return message;
	}

	/**
	 * Sets the {@link SystemExiter}. Useful
	 * for testing.
	 *
	 * @param systemExiter the system exiter
	 */
	public static void presetSystemExiter(SystemExiter systemExiter) {
		AbstractCommandLineRunner.systemExiter = systemExiter;
	}

	/**
	 * Handles the execution of a bean after Application Context(s) has
	 * been initialized. This is considered to be a main entry point
	 * what the application will do after initialization.
	 * <p>
	 * It is implementors responsibility to decide what to do
	 * with the given bean since this class only knows the
	 * typed bean instance.
	 *
	 * @param bean the bean instance
	 * @param parameters the parameters
	 * @param opts the options
	 * @return the exit status
	 */
	protected abstract ExitStatus handleBeanRun(T bean, String[] parameters, Set<String> opts);

	/**
	 * Gets a default bean id which is used to resolve
	 * the instance from an Application Context.
	 *
	 * @return the id of the bean
	 */
	protected abstract String getDefaultBeanIdentifier();

	/**
	 * Gets the list of valid option arguments.
	 * Default implementation returns null thus
	 * not allowing any options exist on a command line.
	 * <p>
	 * When overriding valid options make sure that options
	 * doesn't match anything else planned to be used in
	 * a command line. i.e. usually it's advised to prefix
	 * options with '-' character.
	 *
	 * @return the list of option arguments
	 */
	protected List<String> getValidOpts() {
		return null;
	}

	/**
	 * Allows subclass to modify parsed context configuration path.
	 * Effectively path returned from this method is used
	 * internally for the Application Context config location.
	 * <p>
	 * Default implementation just returns the given
	 * without modifying it.
	 *
	 * @param path the parsed config path
	 * @return the config path
	 */
	protected String getContextConfigPath(String path) {
		return path;
	}

	/**
	 * Allows subclass to modify parsed context configuration path.
	 * Effectively path returned from this method is used
	 * internally for the Application Context config location.
	 * <p>
	 * Default implementation just returns the given
	 * without modifying it.
	 *
	 * @param path the parsed config path
	 * @return the config path
	 */
	protected String getChildContextConfigPath(String path) {
		return path;
	}

	/**
	 * Builds the Application Context(s) and handles 'execution'
	 * of a bean.
	 *
	 * @param configLocation the main context config location
	 * @param masterIdentifier the bean identifier
	 * @param childConfigLocation the child context config location
	 * @param parameters the parameters
	 * @param opts the options
	 * @return the status of the execution
	 */
	protected int start(String configLocation, String masterIdentifier,
			String childConfigLocation, String[] parameters, Set<String> opts) {

		ConfigurableApplicationContext context = null;

		ExitStatus exitStatus = ExitStatus.COMPLETED;
		try {
			context = getApplicationContext(configLocation);
			getChildApplicationContext(childConfigLocation, context);

			@SuppressWarnings("unchecked")
			T bean = (T) context.getBean(masterIdentifier);

			if (log.isDebugEnabled()) {
				log.debug("Passing bean=" + bean + " from context=" + context + " for beanId=" + masterIdentifier);
			}

			exitStatus = handleBeanRun(bean, parameters, opts);

		} catch (Throwable e) {
			e.printStackTrace();
			String message = "Terminated in error: " + e.getMessage();
			log.error(message, e);
			AbstractCommandLineRunner.message = message;
			return exitCodeMapper.intValue(ExitStatus.FAILED.getExitCode());
		} finally {
			if (context != null) {
				context.close();
			}
		}
		return exitCodeMapper.intValue(exitStatus.getExitCode());
	}

	/**
	 * Gets the Application Context.
	 *
	 * @param configLocation the context config location
	 * @return the configured context
	 */
	protected ConfigurableApplicationContext getApplicationContext(String configLocation) {

		ConfigurableApplicationContext context;
		if (ClassUtils.isPresent(configLocation, getClass().getClassLoader())) {
			Class<?> clazz = ClassUtils.resolveClassName(configLocation, getClass().getClassLoader());
			context = new AnnotationConfigApplicationContext(clazz);
		} else {
			context = new ClassPathXmlApplicationContext(configLocation);
		}

		context.getAutowireCapableBeanFactory().autowireBeanProperties(this,
				AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
		return context;
	}

	/**
	 * Gets the Application Context.
	 *
	 * @param configLocation the context config location
	 * @param parent the parent context
	 * @return the configured context
	 */
	protected ConfigurableApplicationContext getChildApplicationContext(
			String configLocation, ConfigurableApplicationContext parent) {
		if (configLocation != null) {
			ConfigurableApplicationContext context =
					new ClassPathXmlApplicationContext(new String[]{configLocation}, parent);
			context.getAutowireCapableBeanFactory().autowireBeanProperties(this,
					AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
			return context;
		} else {
			return null;
		}
	}

	/**
	 * Exit method wrapping handling through
	 * {@link SystemExiter}. This method mostly
	 * exist order to not do a real exit on
	 * a unit tests.
	 *
	 * @param status the exit code
	 */
	public void exit(int status) {
		 systemExiter.exit(status);
	}

	/**
	 * Main method visible to sub-classes.
	 *
	 * @param args the Arguments
	 */
	protected void doMain(String[] args) {

		AbstractCommandLineRunner.message = "";

		// stash normal process arguments
		List<String> newargs = new ArrayList<String>(Arrays.asList(args));

		// read from stdin
		try {
			if (System.in.available() > 0) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				String line = " ";
				while (StringUtils.hasLength(line)) {
					if (!line.startsWith("#") && StringUtils.hasText(line)) {
						log.debug("Stdin arg: " + line);
						newargs.add(line);
					}
					line = reader.readLine();
				}
			}
		} catch (IOException e) {
			log.warn("Could not access stdin (maybe a platform limitation)");
			if (log.isDebugEnabled()) {
				log.debug("Exception details", e);
			}
		}

		Set<String> opts = new HashSet<String>();
		List<String> params = new ArrayList<String>();

		int count = 0;
		String ctxConfigPath = null;
		String childCtxConfigPath = null;
		String beanIdentifier = null;

		// did subclass provide valid opts
		List<String> validOpts = getValidOpts();

		for (String arg : newargs) {
			if (validOpts != null && validOpts.contains(arg)) {
				opts.add(arg);
			} else {
				switch (count) {
				case 0:
					if (!arg.contains("=")) {
						String[] argSplit = arg.split(",");
						ctxConfigPath = argSplit[0];
						if (argSplit.length > 1) {
							childCtxConfigPath = argSplit[1];
						}
					}
					break;
				case 1:
					if (!arg.contains("=")) {
						beanIdentifier = arg;
					} else {
						params.add(arg);
					}
					break;
				default:
					params.add(arg);
					break;
				}
				count++;
			}
		}

		if(beanIdentifier == null) {
			beanIdentifier = getDefaultBeanIdentifier();
		}

		ctxConfigPath = getContextConfigPath(ctxConfigPath);
		childCtxConfigPath = getChildContextConfigPath(childCtxConfigPath);

		if (ctxConfigPath == null || beanIdentifier == null) {
			String message = "At least 2 arguments are required: Context Config and Bean Identifier.";
			log.error(message);
			AbstractCommandLineRunner.message = message;
			exit(1);
		}

		String[] parameters = params.toArray(new String[params.size()]);

		int result = start(ctxConfigPath, beanIdentifier, childCtxConfigPath, parameters, opts);
		exit(result);
	}

}
