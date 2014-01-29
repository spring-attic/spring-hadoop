package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetLastStepExecutionReq extends BaseObject {

	public JobInstanceType jobInstance;
	public String stepName;

	public GetLastStepExecutionReq() {
		super("GetLastStepExecutionReq");
	}

}
