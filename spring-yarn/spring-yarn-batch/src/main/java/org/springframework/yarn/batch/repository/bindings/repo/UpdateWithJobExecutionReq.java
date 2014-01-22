package org.springframework.yarn.batch.repository.bindings.repo;

import org.springframework.yarn.batch.repository.bindings.JobExecutionType;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

public class UpdateWithJobExecutionReq extends BaseObject {

	public JobExecutionType jobExecution;

	public UpdateWithJobExecutionReq() {
		super("UpdateWithJobExecutionReq");

	}
}
