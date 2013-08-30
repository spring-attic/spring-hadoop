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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provide a basic concept of a table structure containing a map of column headers and a collection of rows. Used to
 * render text-based tables (console output).
 *
 * @see UiUtils
 *
 * @author Gunnar Hillert
 * @author Janne Valkealahti
 *
 */
public class Table {

	private final Map<Integer, TableHeader> headers = new TreeMap<Integer, TableHeader>();

	private volatile List<TableRow> rows = new ArrayList<TableRow>(0);

	public List<TableRow> getRows() {
		return rows;
	}

	public void setRows(List<TableRow> rows) {
		this.rows = rows;
	}

	public Map<Integer, TableHeader> getHeaders() {
		return headers;
	}

	public Table addHeader(Integer columnIndex, TableHeader tableHeader) {
		this.headers.put(columnIndex, tableHeader);
		return this;
	}

	/**
	 * Add a new empty row to the table.
	 *
	 * @return the newly created row, which can be then be populated
	 */
	public TableRow newRow() {
		TableRow row = new TableRow();
		rows.add(row);
		return row;
	}

	@Override
	public String toString() {
		return UiUtils.renderTextTable(this);
	}

	public void calculateColumnWidths() {
		for (java.util.Map.Entry<Integer, TableHeader> entry : headers.entrySet()) {
			for (TableRow tableRow : rows) {
				entry.getValue().updateWidth(tableRow.getValue(entry.getKey()).length());
			}
		}
	}

}
