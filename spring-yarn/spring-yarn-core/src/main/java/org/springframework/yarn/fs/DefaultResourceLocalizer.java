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
package org.springframework.yarn.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.YarnSystemException;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

/**
 * Default implementation of {@link ResourceLocalizer} which
 * is only capable of re-using files already in HDFS and preparing
 * correct parameters for created {@link LocalResource} entries.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultResourceLocalizer implements ResourceLocalizer {

	private final static Log log = LogFactory.getLog(DefaultResourceLocalizer.class);

	/** Raw resource transfer entries. */
	private final Collection<TransferEntry> transferEntries;

	/** Raw resource copy entries. */
	private final Collection<CopyEntry> copyEntries;

	/** Yarn configuration, needed to access the hdfs */
	private final Configuration configuration;

	/** Map returned from this instance */
	private Map<String, LocalResource> resources;

	/** Flag if distribution work is done */
	private boolean distributed = false;

	/** Locking the work*/
	private final ReentrantLock distributeLock = new ReentrantLock();

	/** Staging directory */
	private Path stagingDirectory;

	/** The staging id. */
	private String stagingId;

	/** Resolve copy resources */
	private PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

	/**
	 * Instantiates a new default resource localizer.
	 *
	 * @param configuration the configuration
	 * @param transferEntries the transfer entries
	 * @param copyEntries the copy entries
	 */
	public DefaultResourceLocalizer(Configuration configuration, Collection<TransferEntry> transferEntries,
			Collection<CopyEntry> copyEntries) {
		this(configuration, transferEntries, copyEntries, null);
	}

	/**
	 * Instantiates a new default resource localizer.
	 *
	 * @param configuration the configuration
	 * @param transferEntries the transfer entries
	 * @param copyEntries the copy entries
	 * @param stagingDirectory the staging directory
	 */
	public DefaultResourceLocalizer(Configuration configuration, Collection<TransferEntry> transferEntries,
			Collection<CopyEntry> copyEntries, Path stagingDirectory) {
		this.configuration = configuration;
		this.transferEntries = transferEntries;
		this.copyEntries = copyEntries;
		this.stagingDirectory = stagingDirectory;
	}

	@Override
	public Map<String, LocalResource> getResources() {
		if (!distributed) {
			distribute();
		}
		return resources;
	}

	@Override
	public void setStagingDirectory(Path stagingDirectory) {
		this.stagingDirectory = stagingDirectory;
	}

	@Override
	public void setStagingId(String stagingId) {
		this.stagingId = stagingId;
	}

	@Override
	public void distribute() {
		// guard by lock to distribute only once
		distributeLock.lock();
		try {
			if (!distributed) {
				FileSystem fs = FileSystem.get(configuration);
				doFileCopy(fs);
				resources = doFileTransfer(fs);
				distributed = true;
			}
		} catch (IOException e) {
			log.error("Error distributing files", e);
			throw new YarnSystemException("Unable to distribute files", e);
		} catch (URISyntaxException e1) {
			log.error("Error distributing files", e1);
			throw new YarnSystemException("Unable to distribute files", e1);
		} finally {
			distributeLock.unlock();
		}
	}

	@Override
	public boolean clean() {
		return deleteStagingEntries();
	}

	/**
	 * Do file copy.
	 *
	 * @param fs the fs
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void doFileCopy(FileSystem fs) throws IOException {
		for (CopyEntry e : copyEntries) {
			for (String pattern : StringUtils.commaDelimitedListToStringArray(e.src)) {
				for (Resource res : resolver.getResources(pattern)) {
					FSDataOutputStream os = fs.create(getDestinationPath(e, res));
					FileCopyUtils.copy(res.getInputStream(), os);
				}
			}
		}
	}

	/**
	 * Gets the destination path.
	 *
	 * @param entry the entry
	 * @param res the res
	 * @return the destination path
	 * @throws IOException
	 */
	private Path getDestinationPath(CopyEntry entry, Resource res) throws IOException {
		Path resolvedStagingDirectory = resolveStagingDirectory();
		if (resolvedStagingDirectory != null) {
			if (StringUtils.hasText(entry.dest)) {
				return new Path(resolvedStagingDirectory, entry.dest);
			} else {
				return new Path(resolvedStagingDirectory, res.getFilename());
			}
		} else {
			return new Path(entry.dest);
		}
	}

	/**
	 * Gets a map of localized resources.
	 *
	 * @param fs the file system
	 * @return a map of localized resources
	 * @throws IOException if problem occurred getting file status
	 * @throws URISyntaxException if file path is wrong
	 */
	protected Map<String, LocalResource> doFileTransfer(FileSystem fs) throws IOException, URISyntaxException {
		Map<String, LocalResource> returned =  new HashMap<String, LocalResource>();
		Path resolvedStagingDirectory = resolveStagingDirectory();
		for (TransferEntry e : transferEntries) {
			Path remotePath = (!e.staging) ?
					new Path(e.remote + e.path) :
					new Path(e.remote + resolvedStagingDirectory.toUri().getPath() + e.path);
			if(log.isDebugEnabled()) {
				log.debug("Trying path " + remotePath);
			}
			URI localUri = new URI(e.local);
			FileStatus[] fileStatuses = fs.globStatus(remotePath);
			if (!ObjectUtils.isEmpty(fileStatuses)) {
				for(FileStatus status : fileStatuses) {
					if(status.isFile()) {
						URI remoteUri = status.getPath().toUri();
						Path path = new Path(new Path(localUri), remoteUri.getPath());
						LocalResource res = Records.newRecord(LocalResource.class);
						res.setType(e.type);
						res.setVisibility(e.visibility);
						res.setResource(ConverterUtils.getYarnUrlFromPath(path));
						res.setTimestamp(status.getModificationTime());
						res.setSize(status.getLen());
						if(log.isDebugEnabled()) {
							log.debug("Using remote uri [" + remoteUri + "] and local uri [" +
									localUri + "] converted to path [" + path + "]");
						}
						returned.put(status.getPath().getName(), res);
					}
				}
			}
		}
		return returned;
	}

	/**
	 * Resolve runtime staging directory.
	 *
	 * @return the resolved path of runtime stagind directory
	 */
	private Path resolveStagingDirectory() {
		Path base = stagingDirectory != null ?
			stagingDirectory :
			new Path("/" + YarnSystemConstants.DEFAULT_STAGING_BASE_DIR_NAME, YarnSystemConstants.DEFAULT_STAGING_DIR_NAME);
		return stagingId != null ?
			new Path(base, stagingId) :
			base;
	}

	/**
	 * Removes staging entries.
	 *
	 * @return true, if successful
	 */
	private boolean deleteStagingEntries() {
		try {
			FileSystem fs = FileSystem.get(configuration);
			Path resolvedStagingDirectory = resolveStagingDirectory();
			return fs.delete(resolvedStagingDirectory, true);
		} catch (IOException e) {
			return false;
		}
	}

}
