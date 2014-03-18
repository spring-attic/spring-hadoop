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
package org.springframework.yarn.batch.repository;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.OrderComparator;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * Application master service implementation which is used for
 * remote Spring Batch steps to talk to Job Repository. Simply
 * passes requests to {@link JobRepositoryService}.
 *
 * @author Janne Valkealahti
 *
 */
public class BatchAppmasterService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(BatchAppmasterService.class);

	/** Job remote service to use */
	private JobRepositoryService jobRepositoryRemoteService;

	/** Interceptors when communicating with service */
	private final JobRepositoryServiceInterceptorList interceptors =
			new JobRepositoryServiceInterceptorList();

	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		BaseObject baseObject = getConversionService().convert(message, BaseObject.class);
		if(log.isDebugEnabled()) {
			log.debug("Converted message into base object: " + baseObject);
		}

		baseObject = interceptors.preRequest(baseObject);

		BaseResponseObject baseResponseObject;
		if(baseObject == null) {
			baseResponseObject = interceptors.handleRequest(baseObject);
		} else {
			baseResponseObject = jobRepositoryRemoteService.get(baseObject);
		}

		if(log.isDebugEnabled()) {
			log.debug("Response from JobRepositoryRemoteService: " + baseResponseObject);
		}

		baseResponseObject = interceptors.postRequest(baseResponseObject);

		return getConversionService().convert(baseResponseObject, MindRpcMessageHolder.class);
	}

	/**
	 * Sets the job repository remote service.
	 *
	 * @param jobRepositoryRemoteService the new job repository remote service
	 */
	public void setJobRepositoryRemoteService(JobRepositoryService jobRepositoryRemoteService) {
		this.jobRepositoryRemoteService = jobRepositoryRemoteService;
	}

	/**
	 * Set the list of channel interceptors. This will clear any
	 * existing interceptors.
	 *
	 * @param interceptors the new interceptors
	 */
	public void setInterceptors(List<JobRepositoryRemoteServiceInterceptor> interceptors) {
		Collections.sort(interceptors, new OrderComparator());
		this.interceptors.set(interceptors);
	}

	/**
	 * Add a service interceptor to the end of the list.
	 *
	 * @param interceptor the interceptor
	 */
	public void addInterceptor(JobRepositoryRemoteServiceInterceptor interceptor) {
		this.interceptors.add(interceptor);
	}

	/**
	 * Exposes the interceptor list for subclasses.
	 *
	 * @return the interceptors
	 */
	protected JobRepositoryServiceInterceptorList getInterceptors() {
		return this.interceptors;
	}

	/**
	 * Convenient wrapper for interceptor list.
	 */
	protected class JobRepositoryServiceInterceptorList {

		/** Actual list of interceptors */
		private final List<JobRepositoryRemoteServiceInterceptor> interceptors =
				new CopyOnWriteArrayList<JobRepositoryRemoteServiceInterceptor>();

		/**
		 * Sets the interceptors, clears any existing interceptors.
		 *
		 * @param interceptors the list of interceptors
		 * @return <tt>true</tt> if interceptor list changed as a result of the call
		 */
		public boolean set(List<JobRepositoryRemoteServiceInterceptor> interceptors) {
			synchronized (interceptors) {
				interceptors.clear();
				return interceptors.addAll(interceptors);
			}
		}

		/**
		 * Adds interceptor to the list.
		 *
		 * @param interceptor the interceptor
		 * @return <tt>true</tt> (as specified by {@link Collection#add})
		 */
		public boolean add(JobRepositoryRemoteServiceInterceptor interceptor) {
			return interceptors.add(interceptor);
		}

		/**
		 * Handles the pre request calls.
		 *
		 * @param baseObject the base request object
		 * @return the final modified request or <code>null</code> if interceptor broke the chain
		 */
		public BaseObject preRequest(BaseObject baseObject) {
			for (JobRepositoryRemoteServiceInterceptor interceptor : interceptors) {
				baseObject = interceptor.preRequest(baseObject);
				if(baseObject == null) {
					return null;
				}
			}
			return baseObject;
		}

		/**
		 * Handles the handleRequest calls. Method returns immediately
		 * if one of the interceptors chooses to process the request.
		 *
		 * @param baseObject the base request object
		 * @return the processed request if any, <code>null</code> if no processing
		 */
		public BaseResponseObject handleRequest(BaseObject baseObject) {
			BaseResponseObject ret = null;
			for (JobRepositoryRemoteServiceInterceptor interceptor : interceptors) {
				ret = interceptor.handleRequest(baseObject);
				if(ret != null) {
					return ret;
				}
			}
			return ret;
		}

		/**
		 * Handles he post request calls.
		 *
		 * @param baseResponseObject the base response object
		 * @return the final modified response
		 */
		public BaseResponseObject postRequest(BaseResponseObject baseResponseObject) {
			for (JobRepositoryRemoteServiceInterceptor interceptor : interceptors) {
				baseResponseObject = interceptor.postRequest(baseResponseObject);
			}
			return baseResponseObject;
		}

	}

}
