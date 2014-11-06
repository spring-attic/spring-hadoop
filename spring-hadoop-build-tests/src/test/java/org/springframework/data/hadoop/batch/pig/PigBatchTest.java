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
package org.springframework.data.hadoop.batch.pig;

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.hadoop.batch.JobsTrigger;
import org.springframework.data.hadoop.pig.PigServerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertTrue;

/**
 * @author Costin Leau
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/org/springframework/data/hadoop/pig/batch.xml")
public class PigBatchTest {

	@Autowired
	private PigServerFactory pig;
	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testServerNamespace() throws Exception {
		assertTrue(ctx.isPrototype("pig-script"));
		List<JobExecution> startJobs = JobsTrigger.startJobs(ctx);

		// check records
		Collection<StepExecution> steps = startJobs.get(0).getStepExecutions();
		for (StepExecution stepExecution : steps) {
			if ("do-pig".equals(stepExecution.getStepName())) {
				// disable test as Pig runs locally and does not store stats 
				//assertTrue(stepExecution.getReadCount() > 0);
			}
		}

	}

	@Test
	public void testTasklet() throws Exception {
		PigTasklet pt = ctx.getBean("tasklet", PigTasklet.class);
		pt.execute(null, null);
	}
}
