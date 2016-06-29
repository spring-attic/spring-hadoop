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
package org.springframework.data.hadoop.store.support;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.StoreException;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.util.StringUtils;

/**
 * A {@code OutputStoreObjectSupport} is meant to be used from a store
 * {@code DataWriter} implementations by keeping current writing state.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class OutputStoreObjectSupport extends StoreObjectSupport {

	private final static Log log = LogFactory.getLog(OutputStoreObjectSupport.class);

	/** Context holder for strategies */
	private OutputContext outputContext;

	/** Internal flag for initOutputContext */
	private boolean internalInitDone;

	/** Used in-writing suffix if any */
	private String suffix;

	/** Used in-writing prefix if any */
	private String prefix;

	/** Flag guarding if files can be overwritten */
	private boolean overwrite = false;

	/** Flag guarding if file is appended or not */
	private boolean append = false;

	/** Flag enabling Syncable hflush*/
	private boolean syncable = false;

	/**
	 * Instantiates a new abstract output store support.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public OutputStoreObjectSupport(Configuration configuration, Path basePath, CodecInfo codec) {
		super(configuration, basePath, codec);
		this.outputContext = new OutputContext();
		this.outputContext.setCodecInfo(codec);
	}

	@Override
	protected void onInit() throws Exception {
		super.onInit();
		try {
			initOutputContext();
		} catch (Exception e) {
			// silently fail and try again later
		}
	}

	synchronized protected void initOutputContext() throws Exception {
		if (internalInitDone) {
			return;
		}
		for (FileStatus status : findInitFiles(getPath())) {
			String name = status.getPath().getName();
			if (StringUtils.hasText(prefix) && name.startsWith(prefix)) {
				name = name.substring(prefix.length());
			}
			if (StringUtils.hasText(suffix) && name.endsWith(suffix)) {
				name = name.substring(0, name.length() - suffix.length());
			}
			Path path = new Path(status.getPath().getParent(), name);
			if (outputContext.init(path) == null) {
				break;
			}
		}
		internalInitDone = true;
	}

	protected FileStatus[] findInitFiles(Path basePath) throws Exception {
		FileSystem fileSystem = basePath.getFileSystem(getConfiguration());
		if (fileSystem.exists(basePath)) {
			FileStatus[] fileStatuses = fileSystem.listStatus(basePath);
			Arrays.sort(fileStatuses, new Comparator<FileStatus>() {
				public int compare(FileStatus f1, FileStatus f2) {
					// newest first
					return -Long.valueOf(f1.getModificationTime()).compareTo(f2.getModificationTime());
				}
			});
			return fileStatuses;
		} else {
			return new FileStatus[0];
		}
	}

	/**
	 * Gets the strategy context.
	 *
	 * @return the strategy context
	 */
	public OutputContext getOutputContext() {
		if (!internalInitDone) {
			try {
				initOutputContext();
			} catch (Exception e) {
				throw new StoreException("Store output context not yet initialized", e);
			}
		}
		return outputContext;
	}

	/**
	 * Sets the file naming strategy. Default implementation
	 * delegates to {@code StrategyContext}.
	 *
	 * @param fileNamingStrategy the new file naming strategy
	 */
	public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		outputContext.setFileNamingStrategy(fileNamingStrategy);
		outputContext.setCodecInfo(getCodec());
	}

	/**
	 * Sets the rollover strategy. Default implementation
	 * delegates to {@code StrategyContext}.
	 *
	 * @param rolloverStrategy the new rollover strategy
	 */
	public void setRolloverStrategy(RolloverStrategy rolloverStrategy) {
		outputContext.setRolloverStrategy(rolloverStrategy);
	}

	/**
	 * Sets the in writing suffix.
	 *
	 * @param suffix the new in writing suffix
	 */
	public void setInWritingSuffix(String suffix) {
		this.suffix = suffix;
	}

	/**
	 * Sets the in writing prefix.
	 *
	 * @param prefix the new in writing prefix
	 */
	public void setInWritingPrefix(String prefix) {
		this.prefix = prefix;
	}

	/**
	 * Sets the flag indicating if written files may be overwritten.
	 * Default value is <code>FALSE</code> meaning {@code StoreException}
	 * is thrown if file is about to get overwritten.
	 *
	 * @param overwrite the new overwrite
	 */
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
		log.info("Setting overwrite to " + overwrite);
	}

	/**
	 * Checks if overwrite is enabled.
	 *
	 * @return true, if overwrite enabled
	 * @see #setOverwrite(boolean)
	 */
	public boolean isOverwrite() {
		return overwrite;
	}

	/**
	 * Checks if file is appendable
	 *
	 * @return true, if append enabled
	 */
	public boolean isAppendable() {
		return append;
	}

	/**
	 * Set stream as append mode.
	 *
	 * @param append the append flag
	 */
	public void setAppendable(boolean append) {
		this.append = append;
	}

	/**
	 * Checks if syncable is enabled.
	 *
	 * @return true, if syncable is enabled
	 */
	public boolean isSyncable() {
		return syncable;
	}

	/**
	 * Sets the syncable. Enabling will result automatic
	 * call to hdfs Syncable hflush() if available. This
	 * is disable by default.
	 *
	 * @param syncable the syncable flag
	 */
	public void setSyncable(boolean syncable) {
		this.syncable = syncable;
	}

	/**
	 * Gets the resolved path.
	 *
	 * @return the resolved path
	 */
	protected Path getResolvedPath() {
		Path p;
		if (outputContext != null) {
			p = outputContext.resolvePath(getPath());
		} else {
			p = getPath();
		}

		// check for file without inuse prefix/suffix
		if (isFileWriteable(p)) {
			throw new StoreException("Path [" + p + "] exists and overwritten not allowed");
		}

		String name = (StringUtils.hasText(prefix) ? prefix : "") + p.getName()
				+ (StringUtils.hasText(suffix) ? suffix : "");

		p = new Path(p.getParent(), name);
		// check for file with inuse prefix/suffix
		if (isFileWriteable(p)) {
			throw new StoreException("Path [" + p + "] exists and overwritten not allowed");
		}
		return p;
	}

	protected boolean isFileWriteable(Path p){
		return !overwrite && pathExists(p) && !append;
	}

	/**
	 * Sets the write position.
	 *
	 * @param position the new write position
	 */
	protected void setWritePosition(long position) {
		outputContext.setWritePosition(position);
		resetIdleTimeout();
	}

	/**
	 * Rename file using prefix and suffix settings.
	 *
	 * @param path the path to rename
	 * @return path
	 */
	protected Path renameFile(Path path) {
		log.debug("renameFile called with path=[" + path + "]");
		// bail out if there's no in-writing settings
		if (!StringUtils.hasText(prefix) && !StringUtils.hasText(suffix)) {
			return path;
		}
		String name = path.getName();
		if (StringUtils.startsWithIgnoreCase(name, prefix)) {
			name = name.substring(prefix.length());
		}
		if (StringUtils.endsWithIgnoreCase(name, suffix)) {
			name = name.substring(0, name.length() - suffix.length());
		}
		Path toPath = new Path(path.getParent(), name);
		try {
			FileSystem fs = path.getFileSystem(getConfiguration());

			boolean succeed;
			try {
				fs.delete(toPath, false);
				log.info("Renaming path=[" + path + "] toPath=[" + toPath + "]");
				succeed = fs.rename(path, toPath);
			} catch (Exception e) {
				throw new StoreException("Failed renaming from " + path + " to " + toPath, e);
			}
			if (!succeed) {
				throw new StoreException("Failed renaming from " + path + " to " + toPath + " because hdfs returned false");
			}
		}
		catch (IOException e) {
			log.error("Error renaming file", e);
			throw new StoreException("Error renaming file", e);
		}
		return toPath;
	}

	private boolean pathExists(Path path) {
		try {
			return path.getFileSystem(getConfiguration()).exists(path);
		} catch (IOException e) {
		}
		return false;
	}

}
