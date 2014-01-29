package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetLastJobExecutionRes extends BaseResponseObject {

	public JobExecutionType jobExecution;

	public GetLastJobExecutionRes() {
		super("GetLastJobExecutionRes");
	}

}
