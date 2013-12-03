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
package org.springframework.data.hadoop.store.support;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

	/** Used in-writing suffix if any */
	private String suffix;

	/** Used in-writing prefix if any */
	private String prefix;

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
        if (codec != null && outputContext != null) {
            outputContext.setCodecInfo(codec);
        }
    }

    /**
     * Gets the strategy context.
     *
     * @return the strategy context
     */
    public OutputContext getOutputContext() {
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
        String name = (StringUtils.hasText(prefix) ? prefix : "") + p.getName()
        		+ (StringUtils.hasText(suffix) ? suffix : "");
        return new Path(p.getParent(), name);
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
	 */
	protected void renameFile(Path path) {
		// bail out if there's no in-writing settings
		if (!StringUtils.hasText(prefix) && !StringUtils.hasText(suffix)) {
			return;
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
			if (!fs.rename(path, toPath)) {
				throw new StoreException("Failed renaming from " + path + " to " + toPath
						+ " with configuration " + getConfiguration());
			}
		}
		catch (IOException e) {
			log.error("Error renaming file", e);
			throw new StoreException("Error renaming file", e);
		}
	}

}
