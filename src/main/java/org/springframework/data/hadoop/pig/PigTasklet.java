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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.IOUtils;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.Assert;

/**
 * Pig tasklet. Note the same {@link PigServer} is shared between invocations. 
 * 
 * @author Costin Leau
 */
public class PigTasklet implements InitializingBean, Tasklet {

	private PigServer pig;
	private Collection<PigScript> scripts;

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(pig, "A PigServer instance is required");
		Assert.notEmpty(scripts, "At least one script needs to be specified");
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Exception exc = null;

		pig.setBatchOn();
		pig.getPigContext().connect();

		try {
			execute();
			return RepeatStatus.FINISHED;
		} catch (Exception ex) {
			exc = ex;
		}

		throw new HadoopException("Cannot execute Pig script(s)", exc);
	}

	private List<ExecJob> execute() throws IOException {

		// register scripts
		for (PigScript script : scripts) {
			InputStream in = null;
			try {
				in = script.getResource().getInputStream();
				pig.registerScript(in, script.getArguments());
			} finally {
				IOUtils.closeStream(in);
			}
		}

		return pig.executeBatch();
	}

	/**
	 * Sets the pig scripts to be executed by this tasklet.
	 * 
	 * @param scripts The scripts to set.
	 */
	public void setScripts(Collection<PigScript> scripts) {
		this.scripts = scripts;
	}

	/**
	 * Sets the pig server instance used by this tasklet.
	 * 
	 * @param pig The pig to set.
	 */
	public void setPigServer(PigServer pig) {
		this.pig = pig;
	}
}