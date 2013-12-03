/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.store;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.hamcrest.Matchers.lessThan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for tests.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class TestUtils {

	public static void writeData(DataStoreWriter<String> writer, String[] data) throws IOException {
		writeData(writer, data, true);
	}

	public static void writeData(DataStoreWriter<String> writer, String[] data, boolean close) throws IOException {
		for (String line : data) {
			writer.write(line);
		}
		writer.flush();
		if (close) {
			writer.close();
		}
	}

	public static void readDataAndAssert(DataStoreReader<String> reader, String[] expected) throws IOException {
		String line = null;
		int count = 0;
		while ((line = reader.read()) != null) {
			assertThat(count, lessThan(expected.length));
			assertThat(line, is(expected[count++]));
		}
		assertThat(count, is(expected.length));
		line = reader.read();
		assertNull("Expected null, got '" + line + "'", line);
	}

	public static List<String> readData(DataStoreReader<String> reader) throws IOException {
		ArrayList<String> ret = new ArrayList<String>();
		String line = null;
		while ((line = reader.read()) != null) {
			ret.add(line);
		}
		return ret;
	}


}
