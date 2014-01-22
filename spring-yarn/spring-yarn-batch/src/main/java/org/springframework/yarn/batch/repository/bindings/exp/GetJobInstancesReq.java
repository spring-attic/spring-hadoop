package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetJobInstancesReq extends BaseObject {

	public String jobName;
	public Integer start;
	public Integer count;

	public GetJobInstancesReq() {
		super("GetJobInstancesReq");
	}

}
