package org.springframework.yarn.batch.repository.bindings.exp;

import java.util.Set;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

public class FindRunningJobExecutionsRes extends BaseResponseObject {

	public Set<JobExecutionType> jobExecutions;

	public FindRunningJobExecutionsRes() {
		super("FindRunningJobExecutionsRes");
	}

}
