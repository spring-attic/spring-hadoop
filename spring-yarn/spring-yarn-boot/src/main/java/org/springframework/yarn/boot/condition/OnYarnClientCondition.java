/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

/**
 * {@link Condition} that checks for the presence of Hadoop Yarn environment
 * and enable property. Allows to conditionally either force condition even if
 * yarn env is present or not while still falling back to default functionality
 * where presence of yarn environment is checked.
 *
 * @author Janne Valkealahti
 *
 */
class OnYarnClientCondition extends AllNestedConditions {

	OnYarnClientCondition() {
		super(ConfigurationPhase.PARSE_CONFIGURATION);
	}

	private final static String TOKEN_ENV = "HADOOP_TOKEN_FILE_LOCATION";

	@Conditional(ForceYarnClusterCondition.class)
	static class MissingYarn {
	}

	@ConditionalOnProperty(prefix = "spring.yarn.client", name = "enabled", havingValue = "true", matchIfMissing = true)
	static class ForcedOverride {
	}

	private static class ForceYarnClusterCondition extends AnyNestedCondition {

		public ForceYarnClusterCondition() {
			super(ConfigurationPhase.PARSE_CONFIGURATION);
		}

		@Conditional(YarnClusterCondition.class)
		static class ForceYarnClusterConditionConfig {
		}

		@ConditionalOnProperty(prefix = "spring.yarn.client", name = "enabled", havingValue = "true", matchIfMissing = false)
		static class ForceYarnClusterConditionEnabledConfig {
		}
	}

	private static class YarnClusterCondition extends SpringBootCondition {

		@Override
		public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
			String tokenEnv = System.getenv(TOKEN_ENV);
			if (!StringUtils.hasText(tokenEnv)) {
				tokenEnv = System.getProperty(TOKEN_ENV);
			}
			if (tokenEnv == null) {
				return ConditionOutcome.match("Did not found system environment variable " + TOKEN_ENV);
			} else {
				return ConditionOutcome.noMatch("Found system environment variable " + TOKEN_ENV);
			}
		}
	}
}
