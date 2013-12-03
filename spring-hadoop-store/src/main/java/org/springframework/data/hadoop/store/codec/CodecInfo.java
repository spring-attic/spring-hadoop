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
 * Generic interface for supported codec info.
 *
 * @author Janne Valkealahti
 *
 */
public interface CodecInfo {

	/**
	 * Checks if codec is splittable.
	 *
	 * @return true, if is splittable
	 */
	boolean isSplittable();

	/**
	 * Gets the fully qualified name of a codec class.
	 *
	 * @return the codec class
	 */
	String getCodecClass();

	/**
	 * Gets the default suffix.
	 *
	 * @return the default suffix
	 */
	String getDefaultSuffix();

}
