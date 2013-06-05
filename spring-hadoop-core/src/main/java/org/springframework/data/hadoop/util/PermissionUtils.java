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
package org.springframework.data.hadoop.util;

import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.springframework.util.ReflectionUtils;

/**
 * Simple utility class that fixes the File permission problem on 
 * Windows. A temporary work-around until Hadoop fixes this issue out of the box.
 * Needs to be called, once per class-loader, before submitting a job to Hadoop.
 * 
 * @author Costin Leau
 * @author Thomas Risberg
 */
public class PermissionUtils {

	private static final Log log = LogFactory.getLog(PermissionUtils.class);

	private static Class<?> trackerDistributedCacheManagerClass;

	static {
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			try {
				trackerDistributedCacheManagerClass = Class.forName("org.apache.hadoop.filecache.TrackerDistributedCacheManager");
			} catch (ClassNotFoundException e) {
				trackerDistributedCacheManagerClass = null;
				log.warn("Unable to provide Windows jar permission fix: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static void hackHadoopStagingOnWin() {
		// do the assignment only on Windows systems
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			// 0655 = -rwxr-xr-x
			JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0650);
			JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0650);

			if (trackerDistributedCacheManagerClass != null) {
				// handle jar permissions as well
				Field field = ReflectionUtils.findField(trackerDistributedCacheManagerClass, "PUBLIC_CACHE_OBJECT_PERM");
				ReflectionUtils.makeAccessible(field);
				FsPermission perm = (FsPermission) ReflectionUtils.getField(field, null);
				perm.fromShort((short) 0650);
			}
		}
	}
}
