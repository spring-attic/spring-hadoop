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

import java.util.List;
import java.util.concurrent.Future;

import org.springframework.util.concurrent.ListenableFuture;

/**
 * Strategy interface for handling a processing of results
 * resolved from {@link ContainerHandler}s.
 *
 * @author Janne Valkealahti
 *
 */
interface ContainerHandlersResultsProcessor {

	/**
	 * Adding list of results for processing.
	 *
	 * @param results the results
	 */
	void process(List<Object> results);

	/**
	 * Gets the final result wrapped in {@link ResultHolder} containing
	 * actual results and possible {@link Exception}.
	 *
	 * @return the result
	 */
	ResultHolder getResult();

	/**
	 * Cancel all {@link Future}s.
	 */
	void cancel();

	/**
	 * Checks if all {@link ListenableFuture}s have completed
	 * its callbacks.
	 *
	 * @return true, if listenables are done
	 */
	boolean isListenablesDone();

	/**
	 * Sets the listenables complete listener. Existing listener
	 * if set will be replaced.
	 *
	 * @param listener the new listenables complete listener
	 */
	void setListenablesCompleteListener(ListenablesComplete listener);

	/**
	 * Results wrapper having a list of results and possible exception.
	 */
	interface ResultHolder {
		List<Object> getResults();
		Exception getException();
	}

	/**
	 * Interface to listen completion event when {@link ListenableFuture}s,
	 * if any, have completed its callbacks.
	 */
	interface ListenablesComplete {

		/**
		 * This complete method will be called when all
		 * {@link ListenableFuture}s have completed its
		 * callbacks.
		 */
		void complete();
	}

}
