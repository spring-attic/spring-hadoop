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
package org.springframework.yarn.boot.cli;

import java.util.Properties;

import org.springframework.boot.cli.command.AbstractCommand;
import org.springframework.boot.cli.command.status.ExitStatus;
import org.springframework.boot.cli.util.Log;
import org.springframework.yarn.boot.app.YarnInfoApplication;

/**
 * Command listing pushed application from hdfs.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnPushedCommand extends AbstractCommand {

	public YarnPushedCommand() {
		super("pushed", "List pushed applications");
	}

	@Override
	public ExitStatus run(String... args) throws Exception {
		YarnInfoApplication app = new YarnInfoApplication();
		Properties appProperties = new Properties();
		appProperties.setProperty("spring.yarn.internal.YarnInfoApplication.operation", "PUSHED");
		app.appProperties(appProperties);
		String info = app.run(args);
		Log.info(info);
		return ExitStatus.OK;
	}

}
