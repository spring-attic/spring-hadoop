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
package org.springframework.data.hadoop.store.strategy.naming;

import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;

/**
 * Strategy interface for components naming files.
 *
 * @author Janne Valkealahti
 *
 */
public interface FileNamingStrategy {

	/**
	 * Resolve a current filename.
	 *
	 * @return the filename
	 */
	Path resolve(Path path);

	/**
	 * Resets the strategy. This method should be called to prepare next filename in case
	 * strategy doesn't know how to do it automatically.
	 */
	void reset();

	/**
	 * Sets the codec info.
	 *
	 * @param codecInfo the new codec info
	 */
	void setCodecInfo(CodecInfo codecInfo);

}
