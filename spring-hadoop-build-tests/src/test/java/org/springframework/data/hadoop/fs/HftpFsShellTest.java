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

import org.apache.hadoop.fs.FileSystem;
import org.junit.runner.RunWith;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for FsShell using the default file system.
 *
 * @author Costin Leau
 * @Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HftpFsShellTest extends AbstractROFsShellTest {

	public static final String HFTP_FILESYSTEM_CLASS_NAME_FOR_V1_THRU_V2_2 =
			"org.apache.hadoop.hdfs.HftpFileSystem";
	public static final String HFTP_FILESYSTEM_CLASS_NAME_SINCE_V2_3 =
			"org.apache.hadoop.hdfs.web.HftpFileSystem";

	@Override
	Class<? extends FileSystem> fsClass() {
		return getHftpFileSystemClass();
	}

	@SuppressWarnings("unchecked")
	Class<? extends FileSystem> getHftpFileSystemClass() {
		Class<? extends FileSystem> clazz = null;
		try {
			clazz = (Class<? extends FileSystem>) Class.forName(
					HFTP_FILESYSTEM_CLASS_NAME_FOR_V1_THRU_V2_2);
		} catch (ClassNotFoundException e) {
			try {
				clazz = (Class<? extends FileSystem>) Class.forName(
						HFTP_FILESYSTEM_CLASS_NAME_SINCE_V2_3);
			} catch (ClassNotFoundException e1) {
				throw new HadoopException("HftpFileSystem class not available " +
						e1.getMessage(), e1);
			}
		}
		return clazz;
	}
}