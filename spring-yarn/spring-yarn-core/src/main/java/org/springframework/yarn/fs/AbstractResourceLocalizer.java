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
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.YarnSystemException;

/**
 * Base implementation of {@link ResourceLocalizer}.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractResourceLocalizer implements SmartResourceLocalizer {

	private final static Log log = LogFactory.getLog(AbstractResourceLocalizer.class);

	/** Map returned from this instance */
	private Map<String, LocalResource> resources;

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

	/**
	 * Instantiates a new abstract resource localizer.
	 *
	 * @param configuration the configuration
	 */
	public AbstractResourceLocalizer(Configuration configuration) {
		this(configuration, null);
	}

	/**
	 * Instantiates a new abstract resource localizer.
	 *
	 * @param configuration the configuration
	 * @param stagingDirectory the staging directory
	 */
	public AbstractResourceLocalizer(Configuration configuration, Path stagingDirectory) {
		this.configuration = configuration;
		this.stagingDirectory = stagingDirectory;
	}

	@Override
	public Map<String, LocalResource> getResources() {
		if (!isDistributed()) {
			distribute();
		}
		return resources;
	}

	@Override
	public void copy() {
		// guard by lock to copy only once
		getLock().lock();
		try {
			if (!isCopied()) {
				log.info("About to copy localized files");
				FileSystem fs = FileSystem.get(getConfiguration());
				doFileCopy(fs);
				setCopied(true);
			} else {
				log.info("Files already copied");
			}
		} catch (Exception e) {
			log.error("Error copying files", e);
			throw new YarnSystemException("Unable to copy files", e);
		} finally {
			getLock().unlock();
		}
	}

	@Override
	public void distribute() {
		// guard by lock to distribute only once
		getLock().lock();
		try {
			if (!isDistributed()) {
				log.info("About to distribute localized files");
				FileSystem fs = FileSystem.get(getConfiguration());
				if (!isCopied()) {
					doFileCopy(fs);
					setCopied(true);
				} else {
					log.info("Files already copied");
				}
				resources = doFileTransfer(fs);
				setDistributed(true);
			} else {
				log.info("Files already distributed");
			}
		} catch (Exception e) {
			log.error("Error distributing files", e);
			throw new YarnSystemException("Unable to distribute files", e);
		} finally {
			getLock().unlock();
		}
	}

	@Override
	public void resolve() {
		// guard by lock to distribute only once
		getLock().lock();
		try {
			if (!isDistributed()) {
				log.info("About to resolve localized files");
				FileSystem fs = FileSystem.get(getConfiguration());
				resources = doFileTransfer(fs);
				setDistributed(true);
			} else {
				log.info("Files already resolve");
			}
		} catch (Exception e) {
			log.error("Error resolve files", e);
			throw new YarnSystemException("Unable to resolve files", e);
		} finally {
			getLock().unlock();
		}
	}

	@Override
	public boolean clean() {
		return deleteStagingEntries();
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

	/**
	 * Gets the hadoop configuration.
	 *
	 * @return the hadoop configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Gets the lock.
	 *
	 * @return the lock
	 */
	public ReentrantLock getLock() {
		return distributeLock;
	}

	/**
	 * Do file copy.
	 *
	 * @param fs the fs
	 * @throws Exception the exception
	 */
	protected abstract void doFileCopy(FileSystem fs) throws Exception;

	/**
	 * Do file transfer.
	 *
	 * @param fs the fs
	 * @return the map
	 * @throws Exception the exception
	 */
	protected abstract Map<String, LocalResource> doFileTransfer(FileSystem fs) throws Exception;

	/**
	 * Checks if is distributed.
	 *
	 * @return true, if is distributed
	 */
	protected boolean isDistributed() {
		return distributed;
	}

	/**
	 * Sets the distributed.
	 *
	 * @param distributed the new distributed
	 */
	protected void setDistributed(boolean distributed) {
		this.distributed = distributed;
	}

	/**
	 * Checks if is copied.
	 *
	 * @return true, if is copied
	 */
	protected boolean isCopied() {
		return copied;
	}

	/**
	 * Sets the copied.
	 *
	 * @param copied the new copied
	 */
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

	/**
	 * Delete staging entries.
	 *
	 * @return true, if successful
	 */
	protected boolean deleteStagingEntries() {
		if (stagingDirectory == null || !StringUtils.hasText(stagingId)) {
			return false;
		}
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
