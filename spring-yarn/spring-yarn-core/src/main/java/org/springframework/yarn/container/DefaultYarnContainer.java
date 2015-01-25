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
package org.springframework.yarn.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.OrderComparator;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.yarn.container.ContainerHandlersResultsProcessor.ListenablesComplete;
import org.springframework.yarn.container.ContainerHandlersResultsProcessor.ResultHolder;
import org.springframework.yarn.listener.ContainerStateListener.ContainerState;

/**
 * Default implementation of a {@link YarnContainer}.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultYarnContainer extends AbstractYarnContainer implements ApplicationListener<ContextClosedEvent> {

	private final static Log log = LogFactory.getLog(DefaultYarnContainer.class);

	private final ContainerHandlersResultsProcessor processor = new DefaultContainerHandlersResultsProcessor();

	private final AtomicBoolean endNotified = new AtomicBoolean();

	private volatile boolean contextClosing = false;

	@Override
	protected void runInternal() {

		processor.setListenablesCompleteListener(new ListenablesComplete() {
			@Override
			public void complete() {
				log.info("Got ListenablesComplete complete notification");
				if (!contextClosing) {
					ResultHolder result = processor.getResult();
					log.info("About to notifyEndState from a listener callback");
					notifyEndState(result.getResults(), result.getException());
				}
			}
		});

		try {
			// if we don't have any handlers thus meaning no results we
			// explicitly disable handling which should disable possible
			// end state notification and container is left running as is.
			List<ContainerHandler> containerHandlers = getContainerHandlers();
			if (containerHandlers.size() > 0) {
				log.info("Processing " + containerHandlers.size() + " @YarnComponent handlers");
				handleResults(getContainerHandlerResults(containerHandlers));
			} else {
				log.info("Found no @YarnComponent methods, not going to notify end state.");
			}
		} catch (Exception e) {
			log.info("About to notifyEndState from catched exception", e);
			notifyEndState(new ArrayList<Object>(), e);
		}
	}

	@Override
	protected void doStop() {
		log.info("Stopping DefaultYarnContainer and cancelling Futures");
		// need to cancel pending futures

		processor.cancel();
		if (contextClosing) {
			ResultHolder result = processor.getResult();
			log.info("About to notifyEndState from doStop because contextClosing=" + contextClosing);
			notifyEndState(result.getResults(), result.getException());
		}
	}

	@Override
	public boolean isWaitCompleteState() {
		// we need to tell boot ContainerLauncherRunner that we're
		// about to notify state via events so it should wait
		return true;
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		log.info("Setting contextClosing flag because of ContextClosedEvent");
		contextClosing = true;
	}

	private boolean isEmptyValues(List<Object> results) {
		for (Object o : results) {
			if (o != null) {
				if (o instanceof String) {
					if (StringUtils.hasText((String)o)) {
						return false;
					}
				} else {
					return false;
				}
			}
		}
		return true;
	}

	private void notifyEndState(List<Object> results, Exception runtimeException) {
		if (endNotified.getAndSet(true)) {
			log.warn("We already notified end state, discarding this");
			return;
		}
		log.info("Container state based on method results=[" + StringUtils.arrayToCommaDelimitedString(results.toArray())
				+ "] runtimeException=[" + runtimeException + "]");
		if (runtimeException != null) {
			notifyContainerState(ContainerState.FAILED, runtimeException);
		} else if (!isEmptyValues(results)) {
			if (results.size() == 1) {
				notifyContainerState(ContainerState.COMPLETED, results.get(0));
			} else {
				notifyContainerState(ContainerState.COMPLETED, results);
			}
		} else {
			notifyCompleted();
		}
	}

	/**
	 * Handles results by delegating to results processor.
	 *
	 * @param results the results
	 */
	private void handleResults(List<Object> results) {
		processor.process(results);
		if (processor.isListenablesDone()) {
			ResultHolder result = processor.getResult();
			log.info("About to notifyEndState from handleResults because processor is done with listenables");
			notifyEndState(result.getResults(), result.getException());
		}
	}

	/**
	 * Gets the container handler results. This will resolve result objects
	 * from a {@link ContainerHandler}s by calling its {@link ContainerHandler#handle(YarnContainerRuntime)}
	 * methods. Result may be null in case of void method, {@link Exception},
	 * {@link Future}, {@link ListenableFuture} or any other arbitrary {@link Object}.
	 *
	 * @param containerHandlers the container handlers
	 * @return the container handler results
	 */
	private List<Object> getContainerHandlerResults(List<ContainerHandler> containerHandlers) {
		List<Object> results = new ArrayList<Object>();
		for (ContainerHandler handler : containerHandlers) {
			results.add(handler.handle(this));
		}
		return results;
	}

	/**
	 * Resolves all {@link ContainerHandler} beans from a bean factory. Returned
	 * list is sorted using {@link OrderComparator}.
	 *
	 * @return the container handlers
	 */
	private List<ContainerHandler> getContainerHandlers() {
		BeanFactory bf = getBeanFactory();
		Assert.state(bf instanceof ListableBeanFactory, "Bean factory must be instance of ListableBeanFactory");
		Map<String, ContainerHandler> handlers = ((ListableBeanFactory)bf).getBeansOfType(ContainerHandler.class);
		List<ContainerHandler> handlersList = new ArrayList<ContainerHandler>(handlers.values());
		OrderComparator comparator = new OrderComparator();
		Collections.sort(handlersList, comparator);
		return handlersList;
	}

}
