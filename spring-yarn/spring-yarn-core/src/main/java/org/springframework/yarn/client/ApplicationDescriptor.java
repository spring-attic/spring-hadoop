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
package org.springframework.yarn.client;

/**
 * An {@code ApplicationDescriptor} is a descriptor for an application meant to
 * be installed into HDFS and later run from there.
 *
 * @author Janne Valkealahti
 *
 */
public class ApplicationDescriptor {

	// for now we only keep directory to guard
	// against installation and launch errors.
	// we should add more functionality into this
	// descriptor to be able to check before we
	// launch anything that all application files
	// are in place.

	private String directory;
	
	private String name;

	/**
	 * Instantiates a new application descriptor.
	 */
	public ApplicationDescriptor() {
	}

	/**
	 * Instantiates a new application descriptor.
	 *
	 * @param directory the application directory
	 */
	public ApplicationDescriptor(String directory) {
		this(directory, null);
	}
	
	/**
	 * Instantiates a new application descriptor.
	 *
	 * @param directory the application directory
	 * @param name the application name
	 */
	public ApplicationDescriptor(String directory, String name) {
		this.directory = directory;
		this.name = name;
	}

	/**
	 * Gets the application directory.
	 *
	 * @return the application directory
	 */
	public String getDirectory() {
		return directory;
	}

	/**
	 * Sets the application directory.
	 *
	 * @param directory the new application directory
	 */
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	/**
	 * Gets the application name.
	 * 
	 * @return the application name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Sets the application name.
	 * 
	 * @param name the application name
	 */
	public void setName(String name) {
		this.name = name;
	}

}
