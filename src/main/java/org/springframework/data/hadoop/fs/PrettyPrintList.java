/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Dedicated {@link List} with a custom {@link #toString()} to be specified in a template-like fashion. 
 * 
 * @author Costin Leau
 */
@SuppressWarnings("serial")
class PrettyPrintList<E> extends ArrayList<E> {

	interface ListPrinter<E> {
		String toString(E e) throws Exception;
	}

	final ListPrinter<E> printer;

	PrettyPrintList(ListPrinter<E> printer) {
		this.printer = printer;
	}

	PrettyPrintList(int size, ListPrinter<E> printer) {
		super(size);
		this.printer = printer;
	}

	@Override
	public String toString() {
		Iterator<E> i = iterator();
		if (!i.hasNext())
			return "";

		StringBuilder sb = new StringBuilder();
		try {
			for (;;) {
				E e = i.next();
				sb.append(printer.toString(e));
				if (!i.hasNext())
					return sb.toString();
				sb.append("\n");
			}
		} catch (Exception ex) {
			throw new IllegalStateException("Cannot create String representation", ex);
		}
	}
}