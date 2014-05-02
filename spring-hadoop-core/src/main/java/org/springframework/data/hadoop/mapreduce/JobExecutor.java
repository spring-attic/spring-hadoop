/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.hadoop.mapreduce.JobUtils.JobStatus;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Common class shared for executing Hadoop {@link Job}s.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public abstract class JobExecutor implements InitializingBean, DisposableBean, BeanFactoryAware {

	protected interface JobListener {

		Object beforeAction();

		void afterAction(Object state);

		void jobFinished(Job job);

		void jobKilled(Job job);
	}

	private Collection<Job> jobs;
	private Iterable<String> jobNames;
	private boolean waitForCompletion = true;
	private boolean killJobsAtShutdown = true;
	private BeanFactory beanFactory;
	private boolean verbose = true;
	private Executor taskExecutor = new SyncTaskExecutor();

	/** used for preventing exception noise during shutdowns */
	private volatile boolean shuttingDown = false;

	/** jobs alias used during destruction to avoid a BF lookup */
	private Collection<Job> recentJobs = Collections.emptyList();

	protected Log log = LogFactory.getLog(getClass());

	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(jobs != null | jobNames != null, "A Hadoop job or its name is required");

		if (jobNames != null) {
			for (String jobName : jobNames) {
				if (StringUtils.hasText(jobName)) {
					Assert.notNull(beanFactory, "a bean factory is required if the job is specified by name");
					Assert.isTrue(beanFactory.containsBean(jobName), "beanFactory does not contain any bean named ["
							+ jobNames + "]");
				}
			}
		}

		if (isWaitForCompletion()) {
			setKillJobAtShutdown(true);
		}
	}

	@Override
	public void destroy() throws Exception {
		if (isWaitForCompletion() || isKillJobsAtShutdown()) {
			stopJobs();
		}
	}

	/**
	 * Stops running job.
	 * 
	 * @return list of stopped jobs.
	 */
	protected Collection<Job> stopJobs() {
		return stopJobs(null);
	}

	/**
	 * Stops running job.
	 *
	 * @param listener job listener
	 * @return list of stopped jobs.
	 */
	protected Collection<Job> stopJobs(final JobListener listener) {
		shuttingDown = true;

		final Collection<Job> jbs = findJobs();
		final List<Job> killedJobs = new ArrayList<Job>();

		taskExecutor.execute(new Runnable() {
			@Override
			public void run() {

				Object listenerInit = null;
				if (listener != null) {
					listenerInit = listener.beforeAction();
				}

				try {
					for (final Job job : jbs) {
						try {
							if (JobUtils.getStatus(job).isRunning()) {
								synchronized (killedJobs) {
									killedJobs.add(job);
								}
								log.info("Killing job [" + job.getJobName() + "]");
								job.killJob();
								if (listener != null) {
									listener.jobKilled(job);
								}
							}
						} catch (Exception ex) {
							log.warn("Cannot kill job [" + job.getJobName() + "]", ex);
							if (RuntimeException.class.isAssignableFrom(ex.getClass())) {
								throw (RuntimeException)ex;
							} else {
								throw new IllegalStateException(ex);
							}
						}
					}
				} finally {
					if (listener != null) {
						listener.afterAction(listenerInit);
					}
				}
			}
		});

		return jbs;
	}

	protected Collection<Job> startJobs() {
		return startJobs(null);
	}

	protected Collection<Job> startJobs(final JobListener listener) {
		final Collection<Job> jbs = findJobs();

		final List<Job> started = new ArrayList<Job>();

		taskExecutor.execute(new Runnable() {
			@Override
			public void run() {

				Object listenerInit = null;
				if (listener != null) {
					listenerInit = listener.beforeAction();
				}

				try {

					for (final Job job : jbs) {
						boolean succes = false;
						try {
							// job is already running - ignore it
							if (JobUtils.getStatus(job).isStarted()) {
								log.info("Job [" + job.getJobName() + "] already started; skipping it...");
								break;
							}

							log.info("Starting job [" + job.getJobName() + "]");
							synchronized (started) {
								started.add(job);
							}
							if (!waitForCompletion) {
								succes = true;
								job.submit();
							}
							else {
								succes = job.waitForCompletion(verbose);
								log.info("Completed job [" + job.getJobName() + "]");
								if (listener != null) {
									listener.jobFinished(job);
								}

							}
						} catch (InterruptedException ex) {
							log.warn("Job [" + job.getJobName() + "] killed");
							throw new IllegalStateException(ex);
						} catch (Exception ex) {
							log.warn("Cannot start job [" + job.getJobName() + "]", ex);
							throw new IllegalStateException(ex);
						}

						if (!succes) {
							if (!shuttingDown) {
								JobStatus status = JobUtils.getStatus(job);
								if (JobStatus.KILLED == status) {
									throw new IllegalStateException("Job " + job.getJobName() + "] killed");
								}
								else {
									throw new IllegalStateException("Job " + job.getJobName() + "] failed to start; status=" +status);
								}
							}
							else {
								log.info("Job [" + job.getJobName() + "] killed by shutdown");
							}
						}
					}
				} finally {
					if (listener != null) {
						listener.afterAction(listenerInit);
					}
				}
			}
		});

		return started;
	}

	protected Collection<Job> findJobs() {
		Collection<Job> js = null;

		if (jobs != null) {
			js = jobs;
		}

		else {
			if (shuttingDown) {
				return recentJobs;
			}

			js = new ArrayList<Job>();
			for (String name : jobNames) {
				js.add(beanFactory.getBean(name, Job.class));
			}
		}

		recentJobs = js;
		return js;
	}

	/**
	 * Sets the job to execute.
	 * 
	 * @param job The job to execute.
	 */
	public void setJob(Job job) {
		this.jobs = Collections.singleton(job);
	}

	/**
	 * Sets the jobs to execute.
	 * 
	 * @param jobs The job to execute.
	 */
	public void setJobs(Collection<Job> jobs) {
		this.jobs = jobs;
	}

	/**
	 * Sets the jobs to execute by (bean) name. This is the default
	 * method used by the hdp name space to allow lazy initialization and potential scoping
	 * to kick in.
	 * 
	 * @param jobName The job to execute.
	 */
	public void setJobNames(String... jobName) {
		this.jobNames = Arrays.asList(jobName);
	}

	/**
	 * Indicates whether the 'runner' should wait for the job to complete (default).
	 * 
	 * @return whether to wait for the job to complete or not.
	 */
	public boolean isWaitForCompletion() {
		return waitForCompletion;
	}

	/**
	 * Indicates whether the 'runner' should wait for the job to complete (default)
	 * after submission or not.
	 * 
	 * @param waitForJob whether to wait for the job to complete or not.
	 */
	public void setWaitForCompletion(boolean waitForJob) {
		this.waitForCompletion = waitForJob;
	}

	/**
	 * Indicates whether the job execution is verbose (the default) or not.
	 * 
	 * @return whether the job execution is verbose or not.
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * Indicates whether the job execution is verbose (the default) or not.
	 * 
	 * @param verbose whether the job execution is verbose or not.
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	/**
	 * Sets the TaskExecutor used for executing the Hadoop job.
	 * By default, {@link SyncTaskExecutor} is used, meaning the calling thread is used.
	 * While this replicates the Hadoop behavior, it prevents running jobs from being killed if the application shuts down. 
	 * For a fine-tuned control, a dedicated {@link Executor} is recommended. 
	 * 
	 * @param executor the task executor to use execute the Hadoop job.
	 */
	public void setExecutor(Executor executor) {
		Assert.notNull(executor, "a non-null task executor is required");
		this.taskExecutor = executor;
	}

	/**
	 * Indicates whether the configured jobs should be 'killed' when the application
	 * shuts down or not.
	 * 
	 * @return whether or not to kill the configured jobs at shutdown
	 */
	public boolean isKillJobsAtShutdown() {
		return killJobsAtShutdown;
	}

	/**
	 * Indicates whether the configured jobs should be 'killed' when the application
	 * shuts down (default) or not. For long-running or fire-and-forget jobs that live beyond
	 * the starting application, set this to false.
	 * 
	 * Note that if {@link #setWaitForCompletion(boolean)} is true, this flag is considered to be true as otherwise
	 * the application cannot shut down (since it has to keep waiting for the job).
	 * 
	 * @param killJobsAtShutdown whether or not to kill configured jobs when the application shuts down
	 */
	public void setKillJobAtShutdown(boolean killJobsAtShutdown) {
		this.killJobsAtShutdown = killJobsAtShutdown;
	}
}