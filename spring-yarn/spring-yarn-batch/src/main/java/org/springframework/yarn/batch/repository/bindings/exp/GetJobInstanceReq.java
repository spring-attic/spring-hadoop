package org.springframework.yarn.batch.repository.bindings.exp;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class GetJobInstanceReq extends BaseObject {

	public Long instanceId;

	public GetJobInstanceReq() {
		super("GetJobInstanceReq");
	}

}
