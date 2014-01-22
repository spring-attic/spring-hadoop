package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetJobExecutionReq extends BaseObject {

	public Long executionId;

	public GetJobExecutionReq() {
		super("GetJobExecutionReq");
	}

}
