package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class UpdateWithJobExecutionRes extends BaseResponseObject {

	public Long id;
	public Integer version;

	public UpdateWithJobExecutionRes() {
		super("UpdateWithJobExecutionRes");
	}

	public Long getId() {
		return id;
	}

	public Integer getVersion() {
		return version;
	}
}
