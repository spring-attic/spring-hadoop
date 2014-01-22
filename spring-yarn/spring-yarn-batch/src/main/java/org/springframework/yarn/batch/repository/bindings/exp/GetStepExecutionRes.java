package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetStepExecutionRes extends BaseResponseObject {

	public StepExecutionType stepExecution;

	public GetStepExecutionRes() {
		super("GetStepExecutionRes");
	}

}
