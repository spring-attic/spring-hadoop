package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetLastStepExecutionRes extends BaseResponseObject {

	public StepExecutionType stepExecution;

	public GetLastStepExecutionRes() {
		super("GetLastStepExecutionRes");
	}

}
