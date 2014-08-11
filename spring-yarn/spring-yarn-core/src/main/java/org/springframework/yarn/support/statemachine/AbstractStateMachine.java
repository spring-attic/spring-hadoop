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
package org.springframework.yarn.support.statemachine;

import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.yarn.support.LifecycleObjectSupport;
import org.springframework.yarn.support.statemachine.state.State;
import org.springframework.yarn.support.statemachine.transition.Transition;
import org.springframework.yarn.support.statemachine.transition.TransitionKind;
import org.springframework.yarn.support.statemachine.trigger.Trigger;

public abstract class AbstractStateMachine<S, E> extends LifecycleObjectSupport implements StateMachine<State<S,E>, E> {

	private static final Log log = LogFactory.getLog(AbstractStateMachine.class);

	private Collection<State<S,E>> states;

	private Collection<Transition<S,E>> transitions;

	private final State<S,E> initialState;

	private State<S,E> currentState;

	private volatile Runnable task;

	private final Queue<Message<E>> eventQueue = new ConcurrentLinkedQueue<Message<E>>();

	private final LinkedList<Message<E>> deferList = new LinkedList<Message<E>>();

	public AbstractStateMachine(Collection<State<S,E>> states, Collection<Transition<S,E>> transitions, State<S,E> initialState) {
		super();
		this.states = states;
		this.transitions = transitions;
		this.initialState = initialState;
	}

	public Collection<State<S,E>> getStates() {
		return states;
	}

	@Override
	public State<S,E> getState() {
		return currentState;
	}

	@Override
	public State<S,E> getInitialState() {
		return initialState;
	}

	@Override
	public void sendEvent(Message<E> event) {
		event = MessageBuilder.fromMessage(event).setHeader("machine", this).build();
		if (log.isDebugEnabled()) {
			log.debug("Queue event " + event);
		}
		eventQueue.add(event);
		scheduleEventQueueProcessing();
	}

	@Override
	public void sendEvent(E event) {
		sendEvent(MessageBuilder.withPayload(event).build());
	}

	@Override
	protected void doStart() {
		super.doStart();
		switchToState(initialState);
	}

	private void switchToState(State<S,E> state) {
		log.info("Moving into state=" + state + " from " + currentState);
		currentState = state;

		for (Transition<S,E> transition : transitions) {
			State<S,E> source = transition.getSource();
			State<S,E> target = transition.getTarget();
			if (transition.getTrigger() == null && source.equals(currentState)) {
				switchToState(target);
			}

		}

	}

	private void processEventQueue() {
		log.debug("Process event queue");
		Message<E> queuedEvent = null;
		while ((queuedEvent = eventQueue.poll()) != null) {
			Message<E> defer = null;
			for (Transition<S,E> transition : transitions) {
				State<S,E> source = transition.getSource();
				State<S,E> target = transition.getTarget();
				Trigger<S, E> trigger = transition.getTrigger();
				if (source.equals(currentState)) {
					if (trigger != null && trigger.evaluate(queuedEvent.getPayload())) {
						transition.transit(queuedEvent.getHeaders());
						if (transition.getKind() != TransitionKind.INTERNAL) {
							switchToState(target);
						}
						break;
					} else if (source.getDeferredEvents() != null && source.getDeferredEvents().contains(queuedEvent.getPayload())) {
						defer = queuedEvent;
					}
				}
			}
			if (defer != null) {
				log.info("Deferring event " + defer);
				deferList.addLast(defer);
			}
		}
	}

	private void processDeferList() {
		log.debug("Process defer list");
		ListIterator<Message<E>> iterator = deferList.listIterator();
		while (iterator.hasNext()) {
			Message<E> event = iterator.next();
			for (Transition<S,E> transition : transitions) {
				State<S,E> source = transition.getSource();
				State<S,E> target = transition.getTarget();
				Trigger<S, E> trigger = transition.getTrigger();
				if (source.equals(currentState)) {
					if (trigger != null && trigger.evaluate(event.getPayload())) {
						transition.transit(event.getHeaders());
						if (transition.getKind() != TransitionKind.INTERNAL) {
							switchToState(target);
						}
						iterator.remove();
					}
				}
			}
		}
	}

	private void scheduleEventQueueProcessing() {
		if (task == null) {
			task = new Runnable() {
				@Override
				public void run() {
					processEventQueue();
					processDeferList();
					task = null;
				}
			};
			getTaskExecutor().execute(task);
		}
	}

}
