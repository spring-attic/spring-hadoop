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
package org.springframework.yarn.fs;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.ObjectUtils;
import org.springframework.yarn.YarnSystemConstants;

public abstract class AbstractResourceLocalizer implements SmartResourceLocalizer {

	private final static Log log = LogFactory.getLog(AbstractResourceLocalizer.class);

	/** Hadoop configuration */
	private final Configuration configuration;

	/** Staging directory */
	private Path stagingDirectory;

	/** The staging id. */
	private String stagingId;

	/** Flag if distribution work is done */
	private boolean distributed = false;

	/** Flag if copy work is done */
	private boolean copied = false;

	/** Lock for operations */
	private final ReentrantLock distributeLock = new ReentrantLock();

	public AbstractResourceLocalizer(Configuration configuration, Path stagingDirectory) {
		this.configuration = configuration;
		this.stagingDirectory = stagingDirectory;
	}

	@Override
	public void setStagingDirectory(Path stagingDirectory) {
		log.info("Setting stagingDirectory=" + stagingDirectory);
		if (!ObjectUtils.nullSafeEquals(this.stagingDirectory, stagingDirectory)) {
			log.info("Marking distributed state false");
			distributed = false;
			copied = false;
		}
		this.stagingDirectory = stagingDirectory;
	}

	@Override
	public void setStagingId(String stagingId) {
		log.info("Setting stagingId=" + stagingId);
		if (!ObjectUtils.nullSafeEquals(this.stagingId, stagingId)) {
			log.info("Marking distributed state false");
			distributed = false;
			copied = false;
		}
		this.stagingId = stagingId;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public ReentrantLock getLock() {
		return distributeLock;
	}

	protected boolean isDistributed() {
		return distributed;
	}

	protected void setDistributed(boolean distributed) {
		this.distributed = distributed;
	}

	protected boolean isCopied() {
		return copied;
	}

	protected void setCopied(boolean copied) {
		this.copied = copied;
	}

	/**
	 * Resolve runtime staging directory.
	 *
	 * @return the resolved path of runtime stagind directory
	 */
	protected Path resolveStagingDirectory() {
		Path base = stagingDirectory != null ?
			stagingDirectory :
			new Path("/" + YarnSystemConstants.DEFAULT_STAGING_BASE_DIR_NAME, YarnSystemConstants.DEFAULT_STAGING_DIR_NAME);
		return stagingId != null ?
			new Path(base, stagingId) :
			base;
	}

	protected boolean deleteStagingEntries() {
		try {
			FileSystem fs = FileSystem.get(getConfiguration());
			Path resolvedStagingDirectory = resolveStagingDirectory();
			log.info("About to delete staging entries for path=" + resolvedStagingDirectory);
			return fs.delete(resolvedStagingDirectory, true);
		} catch (IOException e) {
			log.error("Error deleting staging entries", e);
			return false;
		} finally {
			setDistributed(false);
			setCopied(false);
		}
	}

}
