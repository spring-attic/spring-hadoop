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
package org.springframework.yarn.support.statemachine.state;

import java.util.Collection;

public abstract class AbstractState<S, E> implements State<S, E> {

	private S id;
	private Collection<E> deferred;

	public AbstractState(S id) {
		this.id = id;
	}

	public AbstractState(S id, Collection<E> deferred) {
		this.id = id;
		this.deferred = deferred;
	}

	@Override
	public S getId() {
		return id;
	}

	@Override
	public Collection<E> getDeferredEvents() {
		return deferred;
	}

	@Override
	public String toString() {
		return "AbstractState [id=" + id + ", deferred=" + deferred + "]";
	}

}
