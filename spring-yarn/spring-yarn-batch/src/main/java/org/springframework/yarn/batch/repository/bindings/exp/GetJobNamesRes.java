package org.springframework.yarn.batch.repository.bindings.exp;

import java.util.List;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetJobNamesRes extends BaseResponseObject {

	public List<String> jobNames;

	public GetJobNamesRes() {
		super("GetJobNamesRes");
	}

}
