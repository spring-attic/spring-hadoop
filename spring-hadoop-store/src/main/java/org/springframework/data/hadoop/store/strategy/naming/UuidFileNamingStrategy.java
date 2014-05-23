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
package org.springframework.data.hadoop.store.strategy.naming;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * A {@code FileNamingStrategy} which uses an uuid.
 *
 * @author Janne Valkealahti
 *
 */
public class UuidFileNamingStrategy extends AbstractFileNamingStrategy {

	private final static Log log = LogFactory.getLog(UuidFileNamingStrategy.class);

	private String uuid;

	private volatile String prefix = "-";

	/**
	 * Instantiates a new uuid file naming strategy using
	 * an {@link UUID}.
	 */
	public UuidFileNamingStrategy() {
		this(UUID.randomUUID().toString());
	}

	/**
	 * Instantiates a new uuid file naming strategy using
	 * a given uuid string.
	 *
	 * @param uuid the uuid string
	 */
	public UuidFileNamingStrategy(String uuid) {
		this.uuid = uuid;
	}

	/**
	 * Instantiates a new uuid file naming strategy using
	 * a given uuid string.
	 *
	 * @param uuid the uuid string
	 * @param enabled the enabled
	 */
	public UuidFileNamingStrategy(String uuid, boolean enabled) {
		this.uuid = uuid;
		setEnabled(enabled);
	}

	@Override
	public Path resolve(Path path) {
		if (isEnabled()) {
			if (path != null) {
				return new Path(path.getParent(), path.getName() + prefix + uuid);
			} else {
				return new Path(prefix + uuid);
			}
		} else {
			return path;
		}
	}

	@Override
	public Path init(Path path) {
		path = super.init(path);
		log.debug("Init using path=[" + path + "]");
		if (path != null && isEnabled()) {
			String oldName = path.getName();
			String newName = StringUtils.replace(oldName, prefix + uuid, "");
			if (!StringUtils.hasText(newName)) {
				path = null;
				log.debug("Removed last handled name part, returning null");
			} else if (!ObjectUtils.nullSafeEquals(oldName, newName)) {
				path = new Path(path.getParent(), newName);
				log.debug("Removed handled prefix, path is now " + newName);
			}
		}
		return path;
	}

	@Override
	public void next() {
	}

	@Override
	public UuidFileNamingStrategy createInstance() {
		UuidFileNamingStrategy instance = new UuidFileNamingStrategy(uuid, isEnabled());
		instance.setOrder(getOrder());
		instance.setEnabled(isEnabled());
		instance.setPrefix(prefix);
		return instance;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getUuid() {
		return uuid;
	}

	/**
	 * Sets the prefix preceding uuid part.
	 *
	 * @param prefix the new prefix
	 */
	public void setPrefix(String prefix) {
		Assert.notNull(prefix, "Prefix cannot be null");
		this.prefix = prefix;
	}

}
