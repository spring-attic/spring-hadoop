package org.springframework.yarn.boot.actuate.endpoint.mvc.domain;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.support.SatisfyStateData;

public class SatisfyStateDataResource {

	private Collection<GridMemberResource> removeData;

	private ContainerAllocateDataResource allocateData;

	public SatisfyStateDataResource() {
	}

	public SatisfyStateDataResource(SatisfyStateData satisfyState) {
		allocateData = new ContainerAllocateDataResource();
		ContainerAllocateData data = satisfyState.getAllocateData();
		allocateData.setAny(data.getAny());
		allocateData.setHosts(data.getHosts());
		allocateData.setRacks(data.getRacks());
		removeData = new ArrayList<GridMemberResource>();
		for (GridMember member : satisfyState.getRemoveData()) {
			removeData.add(new GridMemberResource(member.getId().toString()));
		}
	}

	public void setAllocateData(ContainerAllocateDataResource allocateData) {
		this.allocateData = allocateData;
	}

	public ContainerAllocateDataResource getAllocateData() {
		return allocateData;
	}

	public void setRemoveData(Collection<GridMemberResource> removeData) {
		this.removeData = removeData;
	}

	public Collection<GridMemberResource> getRemoveData() {
		return removeData;
	}

}
