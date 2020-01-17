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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
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
public class DefaultResourceLocalizer extends AbstractResourceLocalizer implements SmartResourceLocalizer {

	private final static Log log = LogFactory.getLog(DefaultResourceLocalizer.class);

	/** Raw resource transfer entries. */
	private final Collection<TransferEntry> transferEntries;

	/** Raw resource copy entries. */
	private final Collection<CopyEntry> copyEntries;

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
		super(configuration, stagingDirectory);
		this.transferEntries = transferEntries;
		this.copyEntries = copyEntries;
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

	@Override
	protected void doFileCopy(FileSystem fs) throws Exception {
		for (CopyEntry e : copyEntries) {
			for (String pattern : StringUtils.commaDelimitedListToStringArray(e.src)) {
				if (log.isDebugEnabled()) {
					log.debug("Searching copy entries using pattern=" + pattern);
				}
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

	@Override
	protected Map<String, LocalResource> doFileTransfer(FileSystem fs) throws Exception {
		Map<String, LocalResource> returned =  new HashMap<String, LocalResource>();
		Path resolvedStagingDirectory = resolveStagingDirectory();
		for (TransferEntry e : transferEntries) {
			Path remotePath = (!e.staging) ?
					new Path(e.path) :
					new Path(resolvedStagingDirectory.toUri().getPath() + e.path);
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
						Path path = status.getPath();
						LocalResource res = Records.newRecord(LocalResource.class);
						res.setType(e.type);
						res.setVisibility(e.visibility);
						res.setResource(URL.fromPath(path));
						res.setTimestamp(status.getModificationTime());
						res.setSize(status.getLen());
						if(log.isDebugEnabled()) {
							log.debug("Using path [" + path + "]");
						}
						returned.put(status.getPath().getName(), res);
					}
				}
			}
		}
		return returned;
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

}
