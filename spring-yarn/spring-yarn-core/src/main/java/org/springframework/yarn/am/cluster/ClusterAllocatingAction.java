/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.am.cluster;

import org.springframework.messaging.MessageHeaders;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.SatisfyStateData;
import org.springframework.yarn.support.statemachine.action.Action;

public class ClusterAllocatingAction implements Action {

	@Override
	public void execute(MessageHeaders headers) {
		ContainerCluster cluster = headers.get("containercluster", ContainerCluster.class);
		ProjectionData projectionData = headers.get("projectiondata", ProjectionData.class);
		AbstractContainerClusterAppmaster appmaster = headers.get("appmaster", AbstractContainerClusterAppmaster.class);

		if(projectionData != null) {
			GridProjection gridProjection = cluster.getGridProjection();
			gridProjection.setProjectionData(projectionData);
		}

		if (cluster != null) {
			SatisfyStateData satisfyData = cluster.getGridProjection().getSatisfyState();
			if (satisfyData != null) {
				appmaster.handleSatisfyStateData(cluster, satisfyData);
			}
		}

	}
}