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
package org.springframework.yarn.test.support;

import org.springframework.yarn.test.YarnTestSystemConstants;

/**
 * Object keeping some testing cluster init info.
 * 
 * @author Janne Valkealahti
 *
 */
public class ClusterInfo {

	private int numYarn;
	private int numDfs;
	private String id;
	
	public ClusterInfo() {
		this(YarnTestSystemConstants.DEFAULT_ID_CLUSTER, 1, 1);
	}
		
	public ClusterInfo(String id, int numYarn, int numDfs) {
		this.id = id;
		this.numYarn = numYarn;
		this.numDfs = numDfs;
	}

	public int getNumYarn() {
		return numYarn;
	}

	public void setNumYarn(int numYarn) {
		this.numYarn = numYarn;
	}

	public int getNumDfs() {
		return numDfs;
	}

	public void setNumDfs(int numDfs) {
		this.numDfs = numDfs;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + numDfs;
		result = prime * result + numYarn;
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
		ClusterInfo other = (ClusterInfo) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (numDfs != other.numDfs)
			return false;
		if (numYarn != other.numYarn)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ClusterInfo [numYarn=" + numYarn + ", numDfs=" + numDfs
				+ ", id=" + id + "]";
	}
	
}
