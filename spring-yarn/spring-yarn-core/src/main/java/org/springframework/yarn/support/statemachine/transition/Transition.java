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
package org.springframework.yarn.support.statemachine.transition;

import java.util.Collection;

import org.springframework.messaging.MessageHeaders;
import org.springframework.yarn.support.statemachine.action.Action;
import org.springframework.yarn.support.statemachine.state.State;
import org.springframework.yarn.support.statemachine.trigger.Trigger;

/**
 * The Interface Transition.
 *
 * @param <S> the generic type
 * @param <E> the element type
 */
public interface Transition<S, E> {

	State<S,E> getSource();

	State<S,E> getTarget();

	Collection<Action> getActions();

	void transit(MessageHeaders headers);

	Trigger<S, E> getTrigger();

	TransitionKind getKind();

}
