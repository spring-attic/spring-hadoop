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
package org.springframework.data.hadoop.store.support;

/**
 * Context keeping information about a current reading operation.
 *
 * @author Janne Valkealahti
 *
 */
public class InputContext {

	private volatile long start;
	private volatile long end;
	private volatile long position;

	public InputContext(long start, long end) {
		this.start = start;
		this.end = end;
		this.position = start;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public boolean isEndReached() {
		return !(getPosition() <= getEnd());
	}

	@Override
	public String toString() {
		return "InputContext [start=" + start + ", end=" + end + ", position=" + position + "]";
	}

}
