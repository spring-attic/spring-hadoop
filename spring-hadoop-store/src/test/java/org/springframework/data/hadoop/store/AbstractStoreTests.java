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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.springframework.data.hadoop.test.junit.AbstractHadoopClusterTests;

/**
 * Base class for store tests.
 *
 * @author Janne Valkealahti
 */
public abstract class AbstractStoreTests extends AbstractHadoopClusterTests {

	protected final static String DATA10 = "0123456789";

	protected final static String DATA11 = "1234567890";

	protected final static String DATA12 = "2345678901";

	protected final static String DATA13 = "3456789012";

	protected final static String DATA14 = "4567890123";

	protected final static String DATA15 = "5678901234";

	protected final static String DATA16 = "6789012345";

	protected final static String DATA17 = "7890123456";

	protected final static String DATA18 = "8901234567";

	protected final static String DATA19 = "9012345678";

	protected final static String[] DATA09ARRAY = new String[] {
		DATA10, DATA11, DATA12, DATA13, DATA14,
		DATA15, DATA16, DATA17, DATA18, DATA19 };

	protected final static String[] DATA04ARRAY = new String[] {
		DATA10, DATA11, DATA12, DATA13, DATA14 };

	protected final static String[] DATA05ARRAY = new String[] {
		DATA10, DATA11, DATA12, DATA13, DATA14, DATA15 };

	protected final static String[] DATA06ARRAY = new String[] {
		DATA10, DATA11, DATA12, DATA13, DATA14, DATA15, DATA16 };

	protected final static String[] DATA59ARRAY = new String[] {
		DATA15, DATA16, DATA17, DATA18, DATA19 };

	protected final static String[] DATA69ARRAY = new String[] {
		DATA16, DATA17, DATA18, DATA19 };

	protected final static String[] DATA79ARRAY = new String[] {
		DATA17, DATA18, DATA19 };

	private static final String tmpDirName = "/tmp/";

	protected final Path testBasePath = new Path(tmpDirName + (tmpDirName.endsWith("/") ? "" : "/")
			+ getClass().getSimpleName());

	protected final Path testDefaultPath = new Path(testBasePath, "default");

	@Before
	public void setup() throws IOException {
		FileSystem fs = testBasePath.getFileSystem(getConfiguration());
		fs.delete(testBasePath, true);
	}

	@After
	public void clean() {
	}


}
