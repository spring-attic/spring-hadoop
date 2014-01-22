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
 * {@link Condition} that checks for the presence of Hadoop Yarn environment.
 *
 * @author Janne Valkealahti
 *
 */
class OnYarnClusterCondition extends SpringBootCondition {

	private final static String TOKEN_ENV = "HADOOP_TOKEN_FILE_LOCATION";

	@Override
	public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
		// TODO: experimental check for HADOOP_TOKEN_FILE_LOCATION env variable
		String tokenEnv = System.getenv(TOKEN_ENV);

		if (metadata.isAnnotated(ConditionalOnYarn.class.getName())) {
			if (tokenEnv != null) {
				return ConditionOutcome.match("@ConditionalOnYarn found system environment variable " + TOKEN_ENV);
			} else {
				return ConditionOutcome.noMatch("@ConditionalOnYarn did not found system environment variable " + TOKEN_ENV);
			}
		}

		if (metadata.isAnnotated(ConditionalOnMissingYarn.class.getName())) {
			if (tokenEnv == null) {
				return ConditionOutcome.match("@ConditionalOnMissingYarn did not found system environment variable " + TOKEN_ENV);
			} else {
				return ConditionOutcome.noMatch("@ConditionalOnMissingYarn found system environment variable " + TOKEN_ENV);
			}
		}

		return ConditionOutcome.match("System environment variable " + TOKEN_ENV + " found");
	}

}
