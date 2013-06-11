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
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;

/**
 * Base implementation for launching Spring context
 * from command line using classpath resources and executing
 * bean methods.
 * <p>
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractCommandLineRunner<T> {

	private static final Log log = LogFactory.getLog(AbstractCommandLineRunner.class);

	private ExitCodeMapper exitCodeMapper = new SimpleJvmExitCodeMapper();

	private static String message = "";

	private static SystemExiter systemExiter = new JvmSystemExiter();

	public static String getErrorMessage() {
		return message;
	}

	public static void presetSystemExiter(SystemExiter systemExiter) {
		AbstractCommandLineRunner.systemExiter = systemExiter;
	}

	protected abstract void handleBeanRun(T bean, String[] parameters, Set<String> opts);

	protected abstract String getDefaultBeanIdentifier();

	protected abstract List<String> getValidOpts();

	protected int start(String configLocation, String masterIdentifier, String[] parameters, Set<String> opts) {

		ConfigurableApplicationContext context = null;

		try {
			context = new ClassPathXmlApplicationContext(configLocation);
			context.getAutowireCapableBeanFactory().autowireBeanProperties(this,
					AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);

			@SuppressWarnings("unchecked")
			T bean = (T) context.getBean(masterIdentifier);
			handleBeanRun(bean, parameters, opts);

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
		return 0;
	}

	public void exit(int status) {
		 systemExiter.exit(status);
	}

	/**
	 *
	 * @param args
	 */
	protected void doMain(String[] args) {

		AbstractCommandLineRunner.message = "";

		List<String> newargs = new ArrayList<String>(Arrays.asList(args));

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
		String beanIdentifier = null;

		List<String> validOpts = getValidOpts();

		for (String arg : newargs) {
			if (validOpts != null && validOpts.contains(arg)) {
				opts.add(arg);
			} else {
				switch (count) {
				case 0:
					ctxConfigPath = arg;
					break;
				case 1:
					beanIdentifier = arg;
					break;
				default:
					params.add(arg);
					break;
				}
				count++;
			}
		}

		// TODO: leaving out beanIdentifier breaks if there are parameters defined

		if(beanIdentifier == null) {
			beanIdentifier = getDefaultBeanIdentifier();
		}

		if (ctxConfigPath == null || beanIdentifier == null) {
			String message = "At least 2 arguments are required: Context Config and Bean Identifier.";
			log.error(message);
			AbstractCommandLineRunner.message = message;
			exit(1);
		}

		String[] parameters = params.toArray(new String[params.size()]);

		int result = start(ctxConfigPath, beanIdentifier, parameters, opts);
		exit(result);
	}

}
