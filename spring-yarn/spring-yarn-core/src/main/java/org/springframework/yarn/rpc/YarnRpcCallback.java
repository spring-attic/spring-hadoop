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
package org.springframework.yarn.rpc;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * Simple helper interface to execute methods via callbacks.
 *
 * @author Janne Valkealahti
 *
 * @param <T> Type of the return value
 * @param <P> Type of the rpc protocol
 */
public interface YarnRpcCallback<T, P> {

	/**
	 * Execute callback.
	 *
	 * @param proxy rpc proxy instance
	 * @return Value returned by callback
	 * @throws YarnRemoteException
	 * @throws RemoteException
	 */
	T doInYarn(P proxy) throws YarnRemoteException, RemoteException;

}
