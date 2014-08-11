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
package org.springframework.yarn.support.statemachine.config.configuration;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.config.common.annotation.AbstractAnnotationConfiguration;
import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurer;
import org.springframework.yarn.support.statemachine.StateMachineSystemConstants;
import org.springframework.yarn.support.statemachine.config.EnumStateMachineFactory;
import org.springframework.yarn.support.statemachine.config.StateMachineConfig;
import org.springframework.yarn.support.statemachine.config.StateMachineFactory;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineConfigBuilder;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineStates;
import org.springframework.yarn.support.statemachine.config.builders.StateMachineTransitions;
import org.springframework.yarn.support.statemachine.state.State;

@Configuration
public class StateMachineFactoryConfiguration<S extends Enum<S>, E extends Enum<E>> extends
		AbstractAnnotationConfiguration<StateMachineConfigBuilder<S, E>, StateMachineConfig<S, E>> {

	StateMachineConfigBuilder<S, E> builder = new StateMachineConfigBuilder<S, E>();

	@Bean(name = StateMachineSystemConstants.DEFAULT_ID_STATEMACHINEFACTORY)
	public StateMachineFactory<State<S, E>, E> stateMachineFactory() {

		StateMachineConfig<S, E> stateMachineConfig = builder.getOrBuild();
		StateMachineTransitions<S, E> stateMachineTransitions = stateMachineConfig.getTransitions();
		StateMachineStates<S, E> stateMachineStates = stateMachineConfig.getStates();

		StateMachineFactory<State<S, E>, E> stateMachineFactory = new EnumStateMachineFactory<S, E>(stateMachineTransitions, stateMachineStates);
		return stateMachineFactory;
	}

	@Override
	protected void onConfigurers(List<AnnotationConfigurer<StateMachineConfig<S, E>, StateMachineConfigBuilder<S, E>>> configurers)	throws Exception {
		for (AnnotationConfigurer<StateMachineConfig<S, E>, StateMachineConfigBuilder<S, E>> configurer : configurers) {
			if (configurer.isAssignable(builder)) {
				builder.apply(configurer);
			}
		}
	}

}