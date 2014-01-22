package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.batch.repository.bindings.JobInstanceType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetJobExecutionsReq extends BaseObject {

	public JobInstanceType jobInstance;

	public GetJobExecutionsReq() {
		super("GetJobExecutionsReq");
	}

}
