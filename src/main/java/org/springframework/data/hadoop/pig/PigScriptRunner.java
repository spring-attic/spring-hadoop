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
package org.springframework.data.hadoop.pig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IOUtils;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;

/**
 * Utility for executing Pig scripts through a {@link PigServer}.
 * 
 * @author Costin Leau
 */
public abstract class PigScriptRunner {


	public static List<ExecJob> run(PigServer pig, Resource script) throws DataAccessException {
		return run(pig, new PigScript(script));
	}

	public static List<ExecJob> run(PigServer pig, PigScript script) throws DataAccessException {
		return run(pig, Collections.singleton(script));
	}

	public static List<ExecJob> run(PigServer pig, Iterable<PigScript> scripts) {
		return run(pig, scripts, pig.getPigContext().getExecutionEngine() == null, true);
	}

	public static List<ExecJob> run(PigServer pig, Iterable<PigScript> scripts, boolean init, boolean close)
			throws DataAccessException {

		if (!pig.isBatchOn()) {
			pig.setBatchOn();
		}

		try {
			if (init) {
				pig.getPigContext().connect();
			}

			InputStream in = null;
			try {
				for (PigScript script : scripts) {
					try {
						in = script.getResource().getInputStream();
					} catch (IOException ex) {
						throw new IllegalArgumentException("Cannot open script [" + script.getResource() + "]", ex);
					}
					pig.registerScript(in, script.getArguments());
				}
			} finally {
				IOUtils.closeStream(in);
			}
			return pig.executeBatch();
		} catch (ExecException ex) {
			throw PigExceptionTranslation.convert(ex);
		} catch (IOException ex) {
			throw PigExceptionTranslation.convert(ex);
		} finally {
			if (close) {
				pig.shutdown();
			}
		}
	}
}