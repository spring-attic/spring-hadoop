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
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.yarn.YarnSystemException;

/**
 * Default implementation of {@link ContainerHandlersResultsProcessor}.
 *
 * @author Janne Valkealahti
 *
 */
class DefaultContainerHandlersResultsProcessor implements ContainerHandlersResultsProcessor {

	private static final Log log = LogFactory.getLog(DefaultContainerHandlersResultsProcessor.class);

	private final List<Result> wrappedResults = new ArrayList<Result>();

	private final AtomicInteger activeListenables = new AtomicInteger();

	private Exception runtimeException = null;

	private ListenablesComplete listener;

	@Override
	public void process(List<Object> results) {
		for (Object result : results) {
			wrappedResults.add(new Result(result));
			if (result instanceof ListenableFuture<?>) {
				activeListenables.incrementAndGet();
			}
		}

		for (final Result wrappedResult : wrappedResults) {
			if (wrappedResult.result instanceof ListenableFuture<?>) {
				((ListenableFuture<?>) wrappedResult.result).addCallback(new ListenableFutureCallback<Object>() {

					@Override
					public void onSuccess(Object result) {
						if (log.isDebugEnabled()) {
							log.info("onSuccess for " + wrappedResult + " with result=[" + result + "]");
						}
						wrappedResult.setResult(result);
						activeListenables.decrementAndGet();
						mayNotifyListener();
					}

					@Override
					public void onFailure(Throwable t) {
						if (log.isDebugEnabled()) {
							log.info("onFailure for " + wrappedResult + " with throwable=[" + t + "]");
						}
						runtimeException = new YarnSystemException("error", t);
						activeListenables.decrementAndGet();
						mayNotifyListener();
					}

				});

			}
		}

	}

	@Override
	public ResultHolder getResult() {
		final List<Object> res = new ArrayList<Object>();
		for (Result r : wrappedResults) {
			try {
				res.add(r.getResult());
			} catch (Exception e) {
				log.debug("Future get() resulted error", e);
			}
		}
		return new ResultHolder() {

			@Override
			public List<Object> getResults() {
				return res;
			}

			@Override
			public Exception getException() {
				return runtimeException;
			}
		};
	}

	@Override
	public void cancel() {
		for (final Result wrappedResult : wrappedResults) {
			try {
				log.info("Cancelling " + wrappedResult);
				wrappedResult.cancelIfFuture();
			} catch (Exception e) {
				log.error("error in cancel", e);
			}
		}
	}

	@Override
	public boolean isListenablesDone() {
		return activeListenables.get() == 0;
	}

	@Override
	public void setListenablesCompleteListener(ListenablesComplete listener) {
		this.listener = listener;
	}

	private void mayNotifyListener() {
		if (activeListenables.get() == 0) {
			if (listener != null) {
				listener.complete();
			}
		}
	}

	/**
	 * Wrapped for result object to make it easier to handle
	 * result as a {@link Future}.
	 */
	private static class Result {

		Object result;

		Result(Object result) {
			this.result = result;
		}

		/**
		 * Request a cancel if result is a {@link Future}.
		 */
		void cancelIfFuture() {
			if (result instanceof Future<?>) {
				((Future<?>)result).cancel(true);
			}
		}

		/**
		 * Gets the result. If result is a future this
		 * method delegates to {@link Future#get()}.
		 *
		 * @return the result
		 */
		Object getResult() {
			if (result instanceof Future<?>) {
				Future<?> f = (Future<?>)result;
				try {
					return f.get();
				} catch (CancellationException e) {
				} catch (InterruptedException e) {
				} catch (ExecutionException e) {
					return new YarnSystemException("Future throwed error", e.getCause());
				}
				return null;
			} else {
				return result;
			}
		}

		/**
		 * Sets the result.
		 *
		 * @param result the new result
		 */
		void setResult(Object result) {
			this.result = result;
		}

	}

}
