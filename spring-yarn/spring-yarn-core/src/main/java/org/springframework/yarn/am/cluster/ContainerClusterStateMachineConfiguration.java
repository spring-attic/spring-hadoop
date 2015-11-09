/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.yarn.am.cluster;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachineFactory;
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineConfigurationConfigurer;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

/**
 * Configuration for state machine build used by {@link AbstractContainerClusterAppmaster}.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@EnableStateMachineFactory
public class ContainerClusterStateMachineConfiguration extends EnumStateMachineConfigurerAdapter<ClusterState, ClusterEvent> {

	@Override
	public void configure(StateMachineConfigurationConfigurer<ClusterState, ClusterEvent> config) throws Exception {
		config
			.withConfiguration()
				.autoStartup(true);
	}

	@Override
	public void configure(StateMachineStateConfigurer<ClusterState, ClusterEvent> states) throws Exception {
		states
			.withStates()
				.initial(ClusterState.INITIAL)
				.state(ClusterState.INITIAL)
				.state(ClusterState.RUNNING)
				.state(ClusterState.ALLOCATING, ClusterEvent.CONFIGURE)
				.state(ClusterState.STOPPING)
				.state(ClusterState.STOPPED)
				.state(ClusterState.DESTROYING)
				.state(ClusterState.DESTROYED)
				.state(ClusterState.FINAL);
	}

	@Override
	public void configure(StateMachineTransitionConfigurer<ClusterState, ClusterEvent> transitions) throws Exception {
		transitions
			.withExternal()
				.source(ClusterState.INITIAL)
				.target(ClusterState.RUNNING)
				.event(ClusterEvent.START)
				.action(clusterStartAction())
				.and()
			.withExternal()
				.source(ClusterState.RUNNING)
				.target(ClusterState.ALLOCATING)
				.event(ClusterEvent.CONFIGURE)
				.action(clusterAllocatingAction())
				.and()
			.withExternal()
				.source(ClusterState.STOPPED)
				.target(ClusterState.RUNNING)
				.event(ClusterEvent.START)
				.and()
			.withExternal()
				.source(ClusterState.ALLOCATING)
				.target(ClusterState.RUNNING)
				.and()
			.withExternal()
				.source(ClusterState.RUNNING)
				.target(ClusterState.STOPPING)
				.event(ClusterEvent.STOP)
				.action(clusterStoppingAction())
				.and()
			.withExternal()
				.source(ClusterState.STOPPING)
				.target(ClusterState.STOPPED)
				.and()
			.withExternal()
				.source(ClusterState.STOPPED)
				.target(ClusterState.DESTROYING)
				.event(ClusterEvent.DESTROY)
				.action(clusterDestroyingAction())
				.and()
			.withExternal()
				.source(ClusterState.INITIAL)
				.target(ClusterState.DESTROYING)
				.event(ClusterEvent.DESTROY)
				.action(clusterDestroyingAction())
				.and()
			.withExternal()
				.source(ClusterState.DESTROYING)
				.target(ClusterState.DESTROYED);
	}

	@Bean
	public ClusterAllocatingAction clusterAllocatingAction() {
		return new ClusterAllocatingAction();
	}

	@Bean
	public ClusterStoppingAction clusterStoppingAction() {
		return new ClusterStoppingAction();
	}

	@Bean
	public ClusterDestroyingAction clusterDestroyingAction() {
		return new ClusterDestroyingAction();
	}

	@Bean
	public ClusterStartAction clusterStartAction() {
		return new ClusterStartAction();
	}

}
