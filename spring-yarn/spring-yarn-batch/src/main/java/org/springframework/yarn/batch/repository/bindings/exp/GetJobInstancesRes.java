package org.springframework.yarn.batch.repository.bindings.exp;

import java.util.List;

import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class GetJobInstancesRes extends BaseResponseObject {

	public List<JobInstanceType> jobInstances;

	public GetJobInstancesRes() {
		super("GetJobInstancesRes");
	}

}
