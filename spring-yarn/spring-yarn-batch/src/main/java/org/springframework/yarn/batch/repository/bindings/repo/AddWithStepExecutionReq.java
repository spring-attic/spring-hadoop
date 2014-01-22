package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.StepExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class AddWithStepExecutionReq extends BaseObject {

	public StepExecutionType stepExecution;

	public AddWithStepExecutionReq() {
		super("AddWithStepExecutionReq");
	}

}
