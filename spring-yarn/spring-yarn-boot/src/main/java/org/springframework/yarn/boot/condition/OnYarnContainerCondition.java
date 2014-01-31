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
package org.springframework.yarn.boot.condition;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * {@link Condition} that checks for the presence of Hadoop Yarn container.
 *
 * @author Janne Valkealahti
 *
 */
class OnYarnContainerCondition extends SpringBootCondition {

	private final static String TOKEN_ENV = "HADOOP_TOKEN_FILE_LOCATION";

	@Override
	public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
		// TODO: should think better way
		String tokenEnv = System.getenv(TOKEN_ENV);
		if (tokenEnv == null) {
			tokenEnv = System.getProperty(TOKEN_ENV);
		}

		if (tokenEnv != null) {
			String[] split = tokenEnv.split("/");
			if (split.length > 2 && !split[split.length-2].endsWith("00001")) {
				return ConditionOutcome.match("Detected container id " + split[split.length-2] + " not ending with '00001'");
			}
		}

		return ConditionOutcome.noMatch("System environment variable " + TOKEN_ENV + " not found");
	}

}
