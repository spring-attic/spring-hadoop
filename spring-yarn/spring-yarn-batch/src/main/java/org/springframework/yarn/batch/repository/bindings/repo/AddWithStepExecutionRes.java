package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class AddWithStepExecutionRes extends BaseResponseObject {

	public Long id;
	public Integer version;

	public AddWithStepExecutionRes() {
		super("AddWithStepExecutionRes");
	}

	public Long getId() {
		return id;
	}

	public Integer getVersion() {
		return version;
	}

}
