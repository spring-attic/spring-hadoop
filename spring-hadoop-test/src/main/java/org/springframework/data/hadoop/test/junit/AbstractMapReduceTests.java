/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.hadoop.test.junit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.data.hadoop.mapreduce.JobUtils;
import org.springframework.data.hadoop.mapreduce.JobUtils.JobStatus;
import org.springframework.util.Assert;

/**
 * Abstract base class providing default functionality
 * for running map reduce tests.
 *
 * @author Janne Valkealahti
 *
 */
public class AbstractMapReduceTests extends AbstractHadoopClusterTests {

	/**
	 * Finds an array of output file {@link Path}s resulted as
	 * mapreduce job run.
	 *
	 * @param outputDirectory the path to jobs output directory
	 * @return list of output files
	 * @throws FileNotFoundException if given path was not found
	 * @throws IOException if general access error occured
	 * @see #getOutputFilePaths(Path)
	 */
	protected Path[] getOutputFilePaths(String outputDirectory) throws FileNotFoundException, IOException {
		return getOutputFilePaths(new Path(outputDirectory));
	}

	/**
	 * Finds an array of output file {@link Path}s resulted as
	 * mapreduce job run.
	 *
	 * @param outputDirectory the path to jobs output directory
	 * @return list of output files
	 * @throws FileNotFoundException if given path was not found
	 * @throws IOException if general access error occured
	 */
	protected Path[] getOutputFilePaths(Path outputDirectory) throws FileNotFoundException, IOException {
		return FileUtil.stat2Paths(getFileSystem().listStatus(outputDirectory,
				new Utils.OutputFileUtils.OutputFilesFilter()));
	}

	/**
	 * Wait finished status. Statuses to wait is one of following,
	 * {@code JobStatus.SUCCEEDED}, {@code JobStatus.FAILED} or {@code JobStatus.KILLED}.
	 *
	 * @param job the job
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @return the job status
	 * @throws Exception if exception occurred
	 */
	protected JobStatus waitFinishedStatus(Job job, long timeout, TimeUnit unit) throws Exception {
		return waitStatus(job, timeout, unit, JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.KILLED);
	}

	/**
	 * Waits state. Returned state is <code>NULL</code>
	 * if something failed or final known state after the wait/poll operations.
	 * Array of job statuses can be used to return immediately from wait
	 * loop if state is matched.
	 *
	 * @param job the job
	 * @param timeout the timeout for wait
	 * @param unit the unit for timeout
	 * @param jobStatuses the job statuses to wait
	 * @return the job status
	 * @throws Exception if exception occurred
	 */
	protected JobStatus waitStatus(Job job, long timeout, TimeUnit unit, JobStatus... jobStatuses) throws Exception {
		Assert.notNull(job, "Hadoop job must be set");

		JobStatus status = null;
		long end = System.currentTimeMillis() + unit.toMillis(timeout);

		// break label for inner loop
		done: do {
			status = findStatus(job);
			if (status == null) {
				break;
			}
			for (JobStatus statusCheck : jobStatuses) {
				if (status.equals(statusCheck)) {
					break done;
				}
			}
			Thread.sleep(1000);
		} while (System.currentTimeMillis() < end);
		return status;
	}

	/**
	 * Find job status.
	 *
	 * @param job the job
	 * @return the job status
	 */
	protected JobStatus findStatus(Job job) {
		return JobUtils.getStatus(job);
	}

}
