package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class UpdateWithStepExecutionRes extends BaseResponseObject {

	public Long id;
	public Integer version;

	public UpdateWithStepExecutionRes() {
		super("UpdateWithStepExecutionRes");
	}

	public Long getId() {
		return id;
	}

	public Integer getVersion() {
		return version;
	}

}
