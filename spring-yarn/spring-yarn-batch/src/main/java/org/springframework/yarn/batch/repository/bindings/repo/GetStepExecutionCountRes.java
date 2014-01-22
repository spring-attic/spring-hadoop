package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetStepExecutionCountRes extends BaseResponseObject {

	public Integer count;

	public GetStepExecutionCountRes() {
		super("GetStepExecutionCountRes");
	}

}
