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
package org.springframework.yarn.am.cluster;

/**
 * Possible states of this machine.
 */
public enum ClusterState {

	/**
	 * Initial state when cluster has been created. Once we enter away from
	 * this state, we never get back from any other state.
	 */
	INITIAL,

	/**
	 * State where cluster is ready to go allocation mode or to be
	 * stopped or destroyed.
	 */
	RUNNING,

	/**
	 * State where cluster is allocating and configuring itself.
	 */
	ALLOCATING,

	/**
	 * State where cluster has been requested to be stopped.
	 */
	STOPPING,

	/**
	 * State where cluster is stopped. Cluster can either be started
	 * or destroyed.
	 */
	STOPPED,

	/**
	 * State when cluster is entering into a mode to be destroyed. This is
	 * similar when cluster is stopped but instead ending into a stopped state,
	 * we end into a destroyed state.
	 */
	DESTROYING,

	/**
	 * State for cluster when it has been destroyed and no further
	 * state changes are allowed except transition to final state.
	 */
	DESTROYED,

	/**
	 * Final state for cluster. Cluster object can be safely removed
	 * and possibly recreated.
	 */
	FINAL
}