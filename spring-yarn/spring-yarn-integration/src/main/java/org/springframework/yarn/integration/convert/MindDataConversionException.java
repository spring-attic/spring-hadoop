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
package org.springframework.yarn.integration.convert;

import org.springframework.core.convert.ConversionException;

/**
 * Simple exception indicating errors during the conversion.
 *
 * @author Janne Valkealahti
 *
 */
@SuppressWarnings("serial")
public class MindDataConversionException extends ConversionException {

	/**
	 * Construct a new mind data conversion exception.
	 *
	 * @param message the exception message
	 * @param cause the cause
	 */
	public MindDataConversionException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Construct a new mind data conversion exception.
	 *
	 * @param message the exception message
	 */
	public MindDataConversionException(String message) {
		super(message);
	}

}
