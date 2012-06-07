/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.hbase;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * AOP interceptor that binds a new Hbase table to the thread before a method call, closing and removing it afterwards in case of any method outcome.
 * If there is already a pre-bound table (from a previous call or transaction), the interceptor simply participates in it.
 * Typically used alongside {@link HbaseSynchronizationManager}.
 * 
 * @author Costin Leau
 */
public class HbaseInterceptor implements MethodInterceptor {
	private static final Log log = LogFactory.getLog(HbaseInterceptor.class);

	private boolean exceptionConversionEnabled = true;

	/**
	 * Sets whether to convert any {@link IOException} raised to a Spring DataAccessException,
	 * compatible with the <code>org.springframework.dao</code> exception hierarchy.
	 * <p>Default is "true". Turn this flag off to let the caller receive raw exceptions
	 * as-is, without any wrapping.
	 * @see org.springframework.dao.DataAccessException
	 */
	public void setExceptionConversionEnabled(boolean exceptionConversionEnabled) {
		this.exceptionConversionEnabled = exceptionConversionEnabled;
	}


	public Object invoke(MethodInvocation methodInvocation) throws Throwable {
		Set<String> names = HbaseSynchronizationManager.getTableNames();

		try {
			Object retVal = methodInvocation.proceed();
			return retVal;
		} catch (Exception ex) {
			if (this.exceptionConversionEnabled) {
				throw convertHBaseException(ex);
			}
			else {
				throw ex;
			}
		} finally {
			Set<String> foundNames = new LinkedHashSet<String>(HbaseSynchronizationManager.getTableNames());
			foundNames.removeAll(names);
			for (String name : foundNames) {
				HTableInterface table = HbaseSynchronizationManager.unbindResource(name);
				try {
					table.close();
				} catch (IOException ex) {
					log.warn("Cannot close table " + name, ex);
				}
			}
		}
	}

	private Exception convertHBaseException(Exception ex) {
		return HbaseUtils.convertHbaseException(ex);
	}
}