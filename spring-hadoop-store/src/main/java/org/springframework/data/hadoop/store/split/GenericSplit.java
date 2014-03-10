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
package org.springframework.data.hadoop.store.split;

/**
 * A {@link GenericSplit} is simple implementation of
 * {@link Split} and {@link SplitLocation} storing needed
 * information as it is.
 *
 * @author Janne Valkealahti
 *
 */
public class GenericSplit implements Split, SplitLocation {

	private long start;
	private long length;
	private String[] locations;

	/**
	 * Instantiates a new generic split.
	 */
	public GenericSplit() {
	}

	/**
	 * Instantiates a new generic split.
	 *
	 * @param start the split start
	 * @param length the split length
	 * @param locations the split locations
	 */
	public GenericSplit(long start, long length, String[] locations) {
		this.start = start;
		this.length = length;
		this.locations = locations;
	}

	@Override
	public long getStart() {
		return start;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public long getEnd() {
		return getStart() + getLength();
	}

	public void setStart(long start) {
		this.start = start;
	}

	public void setLength(long length) {
		this.length = length;
	}

	@Override
	public String[] getLocations() {
		return locations;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (length ^ (length >>> 32));
		result = prime * result + (int) (start ^ (start >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GenericSplit other = (GenericSplit) obj;
		if (length != other.length)
			return false;
		if (start != other.start)
			return false;
		return true;
	}

}