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
package org.springframework.yarn.integration.ip.mind.binding;

/**
 * Base object of messages meant for pojo mapping.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class BaseObject {

	/** Type identifier of the object */
	public String type;

	/**
	 * Constructs an empty object
	 */
	public BaseObject() {
		type = this.getClass().getCanonicalName();
	}

	/**
	 * Constructs object with a given type
	 * @param type the type identifier
	 */
	public BaseObject(String type) {
		this.type = type;
	}

	/**
	 * Get type of this object.
	 * @return type of the object
	 */
	public String getType() {
		return type;
	}

}
