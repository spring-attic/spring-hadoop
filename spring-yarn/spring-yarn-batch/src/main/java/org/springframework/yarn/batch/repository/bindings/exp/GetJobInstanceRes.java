package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetJobInstanceRes extends BaseResponseObject {

	public JobInstanceType jobInstance;

	public GetJobInstanceRes() {
		super("GetJobInstanceRes");
	}

}
