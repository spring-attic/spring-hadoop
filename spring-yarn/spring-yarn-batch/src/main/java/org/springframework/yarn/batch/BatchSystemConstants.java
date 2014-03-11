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
package org.springframework.yarn.batch;

/**
 * Various constants throughout the batch package.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class BatchSystemConstants {

	/** Default key for execution context file name */
	public static final String KEY_FILENAME = "fileName";

	/** Default key for execution context file split start position */
	public static final String KEY_SPLITSTART = "splitStart";

	/** Default key for execution context file split length */
	public static final String KEY_SPLITLENGTH = "splitLength";

	/** Default key for execution context split locations */
	public static final String KEY_SPLITLOCATIONS = "splitLocations";

	/** Default spel expression for step execution context file name */
	public static final String SEC_SPEL_KEY_FILENAME = "#{stepExecutionContext['" + KEY_FILENAME + "']}";

	/** Default spel expression for step execution context file split start position */
	public static final String SEC_SPEL_KEY_SPLITSTART = "#{stepExecutionContext['" + KEY_SPLITSTART + "']}";

	/** Default spel expression for step execution context file split length */
	public static final String SEC_SPEL_KEY_SPLITLENGTH = "#{stepExecutionContext['" + KEY_SPLITLENGTH + "']}";

	/** Default key for input patterns job parameter */
	public static final String KEY_INPUTPATTERNS = "inputPatterns";

	/** Default spel expressiong for input patterns job parameter */
	public static final String JP_SPEL_KEY_INPUTPATTERNS = "#{jobParameters['" + KEY_INPUTPATTERNS + "']}";

	/** Default base key identifying a partition */
	public static final String KEY_PARTITION = "partition";

}
