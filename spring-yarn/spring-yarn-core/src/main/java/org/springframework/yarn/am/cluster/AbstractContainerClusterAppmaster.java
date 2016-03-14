/*
 * Copyright 2014-2015 the original author or authors.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.yarn.am.AbstractEventingAppmaster;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.container.AbstractLauncher;
import org.springframework.yarn.am.grid.Grid;
import org.springframework.yarn.am.grid.GridMember;
import org.springframework.yarn.am.grid.GridProjection;
import org.springframework.yarn.am.grid.GridProjectionFactory;
import org.springframework.yarn.am.grid.GridProjectionFactoryLocator;
import org.springframework.yarn.am.grid.ProjectedGrid;
import org.springframework.yarn.am.grid.listener.ProjectedGridListenerAdapter;
import org.springframework.yarn.am.grid.support.DefaultGrid;
import org.springframework.yarn.am.grid.support.DefaultGridMember;
import org.springframework.yarn.am.grid.support.DefaultProjectedGrid;
import org.springframework.yarn.am.grid.support.ProjectionData;
import org.springframework.yarn.am.grid.support.ProjectionDataRegistry;
import org.springframework.yarn.am.grid.support.SatisfyStateData;
import org.springframework.yarn.am.monitor.ContainerAware;
import org.springframework.yarn.fs.MultiResourceLocalizer;
import org.springframework.yarn.support.PollingTaskSupport;

/**
 * Base implementation of a {@link ContainerClusterAppmaster}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractContainerClusterAppmaster extends AbstractEventingAppmaster implements ContainerClusterAppmaster {

	private static final Log log = LogFactory.getLog(AbstractContainerClusterAppmaster.class);

	/** ClusterId to ContainerCluster map */
	final Map<String, ContainerCluster> clusters = new HashMap<String, ContainerCluster>();

	/** ClusterId to Cluster definition id mapping */
	final Map<String, String> clusterIdToRef = new HashMap<String, String>();

	/** Containers scheduled to be killed */
	private final Queue<Container> killQueue = new LinkedList<Container>();

	/** Grid tracking generic grid members */
	private Grid grid;

	/** Projected grid tracking container clusters */
	ProjectedGrid projectedGrid;

	/** Poller for handling periodic tasks */
	private ClusterTaskPoller clusterTaskPoller;

	/** Factory for building on-demand state machines */
	private StateMachineFactory<ClusterState, ClusterEvent> stateMachineFactory;

	/** Locator for factories creating projections */
	private GridProjectionFactoryLocator gridProjectionFactoryLocator;

	/** Projection data registry */
	private ProjectionDataRegistry projectionDataRegistry;

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		grid = doCreateGrid();
		Assert.notNull(grid, "Grid must be set");
		projectedGrid = doCreateProjectedGrid(grid);
		Assert.notNull(projectedGrid, "ProjectedGrid must be set");
		projectedGrid.addProjectedGridListener(new CommandDispatchListener());

		if (getLauncher() instanceof AbstractLauncher) {
			((AbstractLauncher)getLauncher()).addInterceptor(new ContainerLaunchContextModifyInterceptor());
		}
	}

	@Override
	protected void doStart() {
		super.doStart();
		if (projectionDataRegistry != null) {
			Map<String, ProjectionData> defaults = projectionDataRegistry.getProjectionDatas();
			for (Entry<String, ProjectionData> entry : defaults.entrySet()) {
				// no type means it's missing projection settings
				// so assume it's a blueprint and don't try to create
				// cluster automatically
				if (StringUtils.hasText(entry.getValue().getType())) {
					createContainerCluster(entry.getKey(), entry.getValue());
					startContainerCluster(entry.getKey());
				}
			}
		}
	}

	@Override
	protected void doStop() {
		if (clusterTaskPoller != null) {
			clusterTaskPoller.stop();
			clusterTaskPoller = null;
		}
		super.doStop();
	}

	@Override
	public void submitApplication() {
		log.info("Submitting application");
		registerAppmaster();
		start();
		if(getAllocator() instanceof AbstractAllocator) {
			((AbstractAllocator)getAllocator()).setApplicationAttemptId(getApplicationAttemptId());
		}
		clusterTaskPoller = new ClusterTaskPoller(getTaskScheduler(), getTaskExecutor());
		clusterTaskPoller.init();
		clusterTaskPoller.start();
	}

	@Override
	protected void onContainerAllocated(Container container) {
		if (getMonitor() instanceof ContainerAware) {
			((ContainerAware)getMonitor()).onContainer(Arrays.asList(container));
		}
		DefaultGridMember member = new DefaultGridMember(container);
		if (grid.addMember(member)) {
			ContainerCluster cluster = findContainerClusterByContainer(container);
			if (cluster != null) {
				getLauncher().launchContainer(container, onContainerLaunchCommands(container, cluster, getCommands(clusterIdToRef.get(cluster.getId()))));
			} else {
				getLauncher().launchContainer(container, getCommands());
			}
		} else {
			getAllocator().releaseContainers(Arrays.asList(container));
		}
	}

	@Override
	protected void onContainerLaunched(Container container) {
		if (getMonitor() instanceof ContainerAware) {
			((ContainerAware)getMonitor()).onContainer(Arrays.asList(container));
		}
	}

	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		super.onContainerCompleted(status);

		boolean removed = grid.removeMember(status.getContainerId());
		if (!removed) {
			// force allocation for all clusters if we got completed
			// container unknown to a grid. might be an indication
			// that satisfy states are not met.
			requestAllocationForAll();
		}

		if (getMonitor() instanceof ContainerAware) {
			((ContainerAware)getMonitor()).onContainerStatus(Arrays.asList(status));
		}
	}

	@Override
	public Map<String, ContainerCluster> getContainerClusters() {
		return clusters;
	}

	@Override
	public ContainerCluster createContainerCluster(String clusterId, ProjectionData projectionData) {
		return createContainerCluster(clusterId, clusterId, projectionData, null);
	}

	@Override
	public ContainerCluster createContainerCluster(String clusterId, String clusterDef, ProjectionData projectionData, Map<String, Object> extraProperties) {

		// cluster def not given, assume it is gonna be same as cluster id
		if (clusterDef == null) {
			clusterDef = clusterId;
		}

		GridProjectionFactory gridProjectionFactory = gridProjectionFactoryLocator
				.getGridProjectionFactory(projectionData.getType());
		if (gridProjectionFactory == null) {
			throw new IllegalArgumentException("Projection type " + projectionData.getType()
					+ " not know to gridProjectionFactoryLocator=[" + gridProjectionFactoryLocator + "]");
		}

		// need to merge incoming data with data in registry. this is because data may
		// work as a blueprint thus having some setting but incoming always overrides
		// what already exists.
		ProjectionData p = projectionDataRegistry.getProjectionDatas().get(clusterDef);
		ProjectionData merged = p != null ? p.merge(projectionData) : projectionData;
		if (log.isDebugEnabled()) {
			log.debug("Incoming projection data: " + projectionData);
			log.debug("Blueprint projection data: " + p);
			log.debug("Merged projection data: " + merged);
		}

		GridProjection projection = gridProjectionFactory.getGridProjection(merged, getConfiguration());
		if (projection == null) {
			throw new IllegalArgumentException("Unable to build projection using type " + projectionData.getType());
		}

		StateMachine<ClusterState, ClusterEvent> stateMachine = stateMachineFactory
				.getStateMachine();
		// start here due to spring-statemachine #113
		stateMachine.start();
		DefaultContainerCluster cluster = new DefaultContainerCluster(clusterId, projection, stateMachine, extraProperties);
		clusters.put(cluster.getId(), cluster);
		clusterIdToRef.put(clusterId, clusterDef);
		return cluster;
	}

	@Override
	public void startContainerCluster(String id) {
		ContainerCluster cluster = clusters.get(id);
		if (cluster != null) {
			StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.START)
					.setHeader("containercluster", cluster).setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.CONFIGURE)
					.setHeader("containercluster", cluster).setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
		}
	}

	@Override
	public void stopContainerCluster(String id) {
		ContainerCluster cluster = clusters.get(id);
		if (cluster != null) {
			StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.STOP).setHeader("containercluster", cluster)
					.setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
		}
	}

	@Override
	public void destroyContainerCluster(String id) {
		ContainerCluster cluster = clusters.get(id);
		if (cluster != null) {
			StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.DESTROY)
					.setHeader("containercluster", cluster)
					.setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
		}
	}

	@Override
	public void modifyContainerCluster(String id, ProjectionData data) {
		ContainerCluster cluster = clusters.get(id);
		if (cluster != null) {
			StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.CONFIGURE).setHeader("projectiondata", data)
					.setHeader("containercluster", cluster).setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
		}
	}

	@Autowired
	public void setStateMachineFactory(StateMachineFactory<ClusterState, ClusterEvent> stateMachineFactory) {
		this.stateMachineFactory = stateMachineFactory;
	}

	/**
	 * Sets the {@link GridProjectionFactoryLocator} used to find factories
	 * which are creating an instances of {@link GridProjection}s.
	 *
	 * @param gridProjectionFactoryLocator the grid projection factory locator
	 */
	@Autowired
	public void setGridProjectionFactoryLocator(GridProjectionFactoryLocator gridProjectionFactoryLocator) {
		log.info("Setting gridProjectionFactoryLocator=" + gridProjectionFactoryLocator);
		this.gridProjectionFactoryLocator = gridProjectionFactoryLocator;
	}

	@Autowired(required = false)
	public void setProjectionDataRegistry(ProjectionDataRegistry projectionDataRegistry) {
		log.info("Setting projectionDataRegistry=" + projectionDataRegistry);
		this.projectionDataRegistry = projectionDataRegistry;
	}

	protected Grid doCreateGrid() {
		return new DefaultGrid();
	}

	protected ProjectedGrid doCreateProjectedGrid(Grid grid) {
		return new DefaultProjectedGrid(grid);
	}

	protected void handleSatisfyStateData(ContainerCluster cluster, SatisfyStateData satisfyData) {
		if (satisfyData.getAllocateData() != null) {
			// who set the allocator settings matching id from allocate data???
			// if not configures, fall back to defaults???
			ContainerAllocateData allocateData = satisfyData.getAllocateData();
			// should not set id here!!!
			allocateData.setId(clusterIdToRef.get(cluster.getId()));
			getAllocator().allocateContainers(allocateData);
		}
		for (GridMember member : satisfyData.getRemoveData()) {
			log.info("Queued container to be killed: " + member.getContainer().getId());
			killContainer(member.getContainer());
		}
	}

	protected void killContainer(Container container) {
		killQueue.add(container);
	}

	/**
	 * Called when a container is launched for sub classes to do
	 * a final modifications in these commands. Default implementation
	 * returns commands as is.
	 *
	 * @param container the container
	 * @param cluster the cluster
	 * @param commands the original commands
	 * @return modified commands
	 */
	protected List<String> onContainerLaunchCommands(Container container, ContainerCluster cluster, List<String> commands) {
		return commands;
	}

	/**
	 * Periodic task callback called by a {@link ClusterTaskPoller}.
	 */
	private void doTask() {
		// kill containers not needed
		handleKillQueue();
	}

	/**
	 * Kill all containers from queue.
	 */
	private void handleKillQueue() {
		Container toKill = null;
		while ((toKill = killQueue.poll()) != null) {
			log.info("Killing container: " + toKill);
			getCmTemplate(toKill).stopContainers();
		}
	}

	private ContainerCluster findContainerClusterByContainer(Container container) {
		// TODO: make finding cluster more clever
		for (Entry<String, ContainerCluster> entry : clusters.entrySet()) {
			for (GridMember member : entry.getValue().getGridProjection().getMembers()) {
				if (member.getContainer().equals(container)) {
					return entry.getValue();
				}
			}
		}
		return null;
	}

	private void requestAllocationForAll() {
		for (ContainerCluster cluster : clusters.values()) {
			StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
			stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.CONFIGURE)
					.setHeader("containercluster", cluster)
					.setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
		}

	}

	/**
	 * Internal poller handling cluster allocation request scheduling.
	 */
	private class ClusterTaskPoller extends PollingTaskSupport<Void> {

		public ClusterTaskPoller(TaskScheduler taskScheduler, TaskExecutor taskExecutor) {
			super(taskScheduler, taskExecutor);
		}

		@Override
		protected Void doPoll() {
			doTask();
			return null;
		}

	}

	private class CommandDispatchListener extends ProjectedGridListenerAdapter {

		@Override
		public void memberRemoved(GridProjection projection, GridMember member) {
			log.info("memberRemoved projection=" + projection + " member=" + member);

			for (ContainerCluster cluster : clusters.values()) {
				if (cluster.getGridProjection().equals(projection)) {
					StateMachine<ClusterState, ClusterEvent> stateMachine = cluster.getStateMachine();
					stateMachine.sendEvent(MessageBuilder.withPayload(ClusterEvent.CONFIGURE)
							.setHeader("containercluster", cluster).setHeader("appmaster", AbstractContainerClusterAppmaster.this).build());
				}
			}

		}
	}

	protected Map<String, LocalResource> buildLocalizedResources(ContainerCluster cluster) {
		if (getResourceLocalizer() instanceof MultiResourceLocalizer) {
			if (cluster != null) {
				MultiResourceLocalizer loc = (MultiResourceLocalizer) getResourceLocalizer();
				Map<String, LocalResource> resources = loc.getResources(clusterIdToRef.get(cluster.getId()));
				return resources;
			}
		} else {
			log.warn("Can't use container specific local resources because MultiResourceLocalizer expected instead of "
					+ getResourceLocalizer());
		}
		return new HashMap<String, LocalResource>();
	}

	private class ContainerLaunchContextModifyInterceptor implements ContainerLauncherInterceptor {

		@Override
		public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
			ContainerCluster cluster = findContainerClusterByContainer(container);
			if (cluster != null) {
				Map<String, LocalResource> resources = buildLocalizedResources(cluster);
				context.setLocalResources(resources);
				Map<String, String> environment = new HashMap<String, String>(context.getEnvironment());
				environment.putAll(getEnvironment(clusterIdToRef.get(cluster.getId())));
				context.setEnvironment(environment);
			}
			return context;
		}
	}

}
