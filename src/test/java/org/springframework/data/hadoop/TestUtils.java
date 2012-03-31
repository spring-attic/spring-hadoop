/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

/**
 * @author Costin Leau
 */
public class TestUtils {

	public static void hackHadoopStagingOnWin() {
		// do the assignment only on Windows systems
		if (System.getProperty("os.name").startsWith("Windows")) {
			// 0655 = -rwxr-xr-x
			JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0655);
			JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0655);
		}
	}

	public static Resource mkdir(Configuration cfg, String dir) {
		HdfsResourceLoader loader = new HdfsResourceLoader(cfg);
		try {
			Resource resource = loader.getResource(dir);
			FileSystem fs = ((HdfsResourceLoader) loader).getFileSystem();
			fs.mkdirs(new Path(resource.getURI()));
			return loader.getResource(dir);
		} catch (IOException ex) {
			throw new IllegalArgumentException(ex);
		}

	}

	public static Resource writeToFS(Configuration cfg, String filename) {
		return writeToFS(new HdfsResourceLoader(cfg), filename);
	}

	public static Resource writeToFS(HdfsResourceLoader loader, String filename) {
		try {
			Resource resource = loader.getResource(filename);
			//System.out.println("Writing resource " + resource.getURI());
			//WritableResource wr = (WritableResource) resource;

			FileSystem fs = ((HdfsResourceLoader) loader).getFileSystem();

			byte[] bytes = filename.getBytes();
			OutputStream out = fs.create(new Path(resource.getURI()));
			out.write(bytes);
			out.close();

			return loader.getResource(filename);
		} catch (IOException ex) {
			throw new IllegalArgumentException(ex);
		}
	}

}