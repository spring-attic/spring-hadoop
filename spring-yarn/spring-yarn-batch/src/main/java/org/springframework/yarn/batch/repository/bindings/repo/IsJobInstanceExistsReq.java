package org.springframework.yarn.batch.repository.bindings.repo;

import java.util.Map;

import org.springframework.yarn.batch.repository.bindings.JobParameterType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class IsJobInstanceExistsReq extends BaseObject {

	public String jobName;
	public Map<String, JobParameterType> jobParameters;

	public IsJobInstanceExistsReq() {
		super("IsJobInstanceExistsReq");
	}

}
