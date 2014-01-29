package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class UpdateWithStepExecutionReq extends BaseObject {

	public StepExecutionType stepExecution;

	public UpdateWithStepExecutionReq() {
		super("UpdateWithStepExecutionReq");
	}

}
