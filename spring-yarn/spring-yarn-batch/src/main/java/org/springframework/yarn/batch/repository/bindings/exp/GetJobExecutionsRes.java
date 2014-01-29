package org.springframework.yarn.batch.repository.bindings.exp;

import java.util.List;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetJobExecutionsRes extends BaseResponseObject {

	public List<JobExecutionType> jobExecutions;

	public GetJobExecutionsRes() {
		super("GetJobExecutionsRes");
	}

}
