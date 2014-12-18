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
package org.springframework.data.hadoop.store.codec;

/**
 * A default {@code CodecInfo} implementation. Contains a static information about
 * the codec class and its ability being a splittable.
 *
 * @author Janne Valkealahti
 *
 */
public class DefaultCodecInfo implements CodecInfo {

	private final boolean splittable;

	private final String clazz;

	private final String suffix;

	/**
	 * Instantiates a new default codec info.
	 *
	 * @param clazz the clazz of a codec
	 * @param splittable the info if codec is splittable
	 * @param suffix the suffix
	 */
	public DefaultCodecInfo(String clazz, boolean splittable, String suffix) {
		super();
		this.splittable = splittable;
		this.clazz = clazz;
		this.suffix = suffix;
	}

	@Override
	public boolean isSplittable() {
		return splittable;
	}

	@Override
	public String getCodecClass() {
		return clazz;
	}

	@Override
	public String getDefaultSuffix() {
		return suffix;
	}

}
