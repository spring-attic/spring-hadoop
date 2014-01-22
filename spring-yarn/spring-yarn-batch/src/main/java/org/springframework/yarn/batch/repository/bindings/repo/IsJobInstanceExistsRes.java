package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class IsJobInstanceExistsRes extends BaseResponseObject {

	public Boolean response;

	public IsJobInstanceExistsRes() {
		super("IsJobInstanceExistsRes");
	}

	public IsJobInstanceExistsRes(Boolean response) {
		this();
		this.response = response;
	}

}
