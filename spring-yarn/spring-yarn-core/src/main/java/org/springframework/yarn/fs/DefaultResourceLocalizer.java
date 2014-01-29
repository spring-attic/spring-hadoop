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
import java.util.Map.Entry;
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

	/** Flag if copy work is done */
	private boolean copied = false;

	/** Locking the work*/
	private final ReentrantLock distributeLock = new ReentrantLock();

	/** Staging directory */
	private Path stagingDirectory;

	/** The staging id. */
	private String stagingId;

	/**
	 * Contents of a raw byte arrays with mapping to file names. These
	 * are useful for passing in small configuration files which should
	 * be copied from memory instead from a real files.
	 */
	private Map<String, byte[]> rawFileContents = new HashMap<String, byte[]>();

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

	@Override
	public void copy() {
		// guard by lock to copy only once
		distributeLock.lock();
		try {
			if (!copied) {
				log.info("About to copy localized files");
				FileSystem fs = FileSystem.get(configuration);
				doFileCopy(fs);
				copied = true;
			} else {
				log.info("Files already copied");
			}
		} catch (IOException e) {
			log.error("Error copying files", e);
			throw new YarnSystemException("Unable to copy files", e);
		} finally {
			distributeLock.unlock();
		}
	}

	@Override
	public void distribute() {
		// guard by lock to distribute only once
		distributeLock.lock();
		try {
			if (!distributed) {
				log.info("About to distribute localized files");
				FileSystem fs = FileSystem.get(configuration);
				if (!copied) {
					doFileCopy(fs);
					copied = true;
				} else {
					log.info("Files already copied");
				}
				resources = doFileTransfer(fs);
				distributed = true;
			} else {
				log.info("Files already distributed");
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
	public void resolve() {
		// guard by lock to distribute only once
		distributeLock.lock();
		try {
			if (!distributed) {
				log.info("About to resolve localized files");
				FileSystem fs = FileSystem.get(configuration);
				resources = doFileTransfer(fs);
				distributed = true;
			} else {
				log.info("Files already resolve");
			}
		} catch (IOException e) {
			log.error("Error resolve files", e);
			throw new YarnSystemException("Unable to resolve files", e);
		} catch (URISyntaxException e1) {
			log.error("Error resolving files", e1);
			throw new YarnSystemException("Unable to resolve files", e1);
		} finally {
			distributeLock.unlock();
		}
	}

	@Override
	public boolean clean() {
		return deleteStagingEntries();
	}

	/**
	 * Adds a content into a to be written entries.
	 *
	 * @param key the key considered as a file name
	 * @param value the content of a file to be written
	 * @return true, existing content for key already exist and was replaced
	 */
	public boolean AddRawContent(String key, byte[] value) {
		return rawFileContents.put(key, value) != null;
	}

	/**
	 * Sets the raw file contents. Overwrites all existing contents.
	 *
	 * @param rawFileContents the raw file contents
	 */
	public void setRawFileContents(Map<String, byte[]> rawFileContents) {
		this.rawFileContents = rawFileContents;
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
					Path destinationPath = getDestinationPath(e, res);
					if (log.isDebugEnabled()) {
						log.debug("For pattern=" + pattern + " found res=" + res + " destinationPath=" + destinationPath);
					}
					FSDataOutputStream os = fs.create(destinationPath);
					int bytes = FileCopyUtils.copy(res.getInputStream(), os);
					if (log.isDebugEnabled()) {
						log.debug("bytes copied:" + bytes);
					}
				}
			}
		}

		if (rawFileContents != null) {
			Path resolvedStagingDirectory = resolveStagingDirectory();
			for (Entry<String, byte[]> entry : rawFileContents.entrySet()) {
				Path path = new Path(resolvedStagingDirectory, entry.getKey());
				FSDataOutputStream os = fs.create(path);
				if (log.isDebugEnabled()) {
					log.debug("Creating a file " + path);
				}
				FileCopyUtils.copy(entry.getValue(), os);
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
		Path dest = null;
		Path resolvedStagingDirectory = resolveStagingDirectory();
		if (entry.staging) {
			if (StringUtils.hasText(entry.dest)) {
				dest = new Path(resolvedStagingDirectory, entry.dest);
			} else {
				dest = new Path(resolvedStagingDirectory, res.getFilename());
			}
		} else {
			dest =  new Path(entry.dest, res.getFilename());
		}
		if (log.isDebugEnabled()) {
			log.debug("Copy for resource=[" + res + "] dest=[" + dest + "]" + " resolvedStagingDirectory=" + resolvedStagingDirectory);
		}
		return dest;
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
			URI localUri = new URI(e.local);
			FileStatus[] fileStatuses = fs.globStatus(remotePath);
			if(log.isDebugEnabled()) {
				log.debug("Trying path " + remotePath + " glob fileStatus length=" + (fileStatuses != null ? fileStatuses.length : "null"));
			}
			if (!ObjectUtils.isEmpty(fileStatuses)) {
				for(FileStatus status : fileStatuses) {
					if(log.isDebugEnabled()) {
						log.debug("FileStatus=" + status);
					}
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
			log.info("About to delete staging entries for path=" + resolvedStagingDirectory);
			return fs.delete(resolvedStagingDirectory, true);
		} catch (IOException e) {
			log.error("Error deleting staging entries", e);
			return false;
		} finally {
			distributed = false;
			copied = false;
		}
	}

}
