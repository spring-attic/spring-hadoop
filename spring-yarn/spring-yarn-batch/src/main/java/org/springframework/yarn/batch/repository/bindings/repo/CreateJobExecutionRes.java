package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class CreateJobExecutionRes extends BaseResponseObject {

	public JobExecutionType jobExecution;

	public CreateJobExecutionRes() {
		super("CreateJobExecutionRes");
	}

}
