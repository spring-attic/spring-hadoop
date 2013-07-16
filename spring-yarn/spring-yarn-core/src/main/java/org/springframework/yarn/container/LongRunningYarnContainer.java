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
package org.springframework.yarn.container;

import org.springframework.yarn.listener.ContainerStateListener;

/**
 * Extension of {@link YarnContainer} indicating that
 * container is intended to be a long running operation.
 * Usually container will do its tasks and goes away by
 * its own, but in some use cases it is appropriate to
 * allow container to be kept up and running by hooking
 * to its states.
 * <p>
 * Usual case is to ask from method {@link #isWaitCompleteState()}
 * if runner should wait complete state via listener
 * which is registered using method
 * {@link #addContainerStateListener(ContainerStateListener)}.
 *
 * @author Janne Valkealahti
 *
 */
public interface LongRunningYarnContainer extends YarnContainer {


	/**
	 * Adds the container state listener.
	 *
	 * @param listener the {@link ContainerStateListener}
	 */
	void addContainerStateListener(ContainerStateListener listener);

	/**
	 * Indication for possible handler using this bean whether
	 * it should wait {@link org.springframework.yarn.listener.ContainerStateListener.ContainerState#COMPLETED COMPLETED}
	 * state.
	 *
	 * @return True if complete state should be wait
	 * @see #addContainerStateListener(ContainerStateListener)
	 */
	boolean isWaitCompleteState();

}
