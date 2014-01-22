package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetStepExecutionReq extends BaseObject {

	public Long jobExecutionId;

	public Long stepExecutionId;

	public GetStepExecutionReq() {
		super("GetStepExecutionReq");
	}

}
