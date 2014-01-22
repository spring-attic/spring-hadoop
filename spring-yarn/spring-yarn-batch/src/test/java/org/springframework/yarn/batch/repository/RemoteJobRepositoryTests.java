package org.springframework.yarn.batch.repository;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.yarn.batch.repository.RemoteJobRepository;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration
//@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class RemoteJobRepositoryTests extends AbstractJobRepositoryTests {

	@Override
	protected JobRepository getJobRepository() {
		return new RemoteJobRepository(new StubAppmasterScOperations());
	}

}
