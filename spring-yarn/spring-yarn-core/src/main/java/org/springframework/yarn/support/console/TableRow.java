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
package org.springframework.yarn.support.console;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds the table rows used by {@link Table}.
 *
 * @see UiUtils
 *
 * @author Gunnar Hillert
 * @author Ilayaperumal Gopinathan
 * @author Janne Valkealahti
 *
 */
public class TableRow {

	/** Holds the data for the column */
	private Map<Integer, String> data = new HashMap<Integer, String>();

	public void setData(Map<Integer, String> data) {
		this.data = data;
	}

	/**
	 * Return a value from this row.
	 *
	 * @param key Column for which to return the value for
	 * @return Value of the specified column within this row
	 *
	 */
	public String getValue(Integer key) {
		return data.get(key);
	}

	/**
	 * Add a value to the to the specified column within this row.
	 *
	 * @param column
	 * @param value
	 */
	public TableRow addValue(Integer column, String value) {
		this.data.put(column, value);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((data == null) ? 0 : data.hashCode());
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
		TableRow other = (TableRow) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		}
		else if (!data.equals(other.data))
			return false;
		return true;
	}

}
