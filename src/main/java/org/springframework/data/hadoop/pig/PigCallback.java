/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.pig;

import java.io.IOException;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * Callback interface for Pig code. To be used with {@link PigTemplate} execute method, assumably often as anonymous classes within a method implementation.
 * 
 * @author Costin Leau
 */
public interface PigCallback<T> {

	/**
	 * Gets called by {@link PigTemplate#execute(PigCallback)} with an active {@link PigServer}. Does not need to care about activating or closing the {@link PigServer}, or handling exceptions.
	 * 
	 * @param pig active pig server
	 * @return action result
	 * @throws ExecException if thrown by Pig API
	 * @throws IOException if thrown by Pig API
	 */
	T doInPig(PigServer pig) throws ExecException, IOException;
}
