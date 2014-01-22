package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetJobExecutionRes extends BaseResponseObject {

	public JobExecutionType jobExecution;

	public GetJobExecutionRes() {
		super("GetJobExecutionRes");
	}

}
