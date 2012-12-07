/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.springframework.core.io.Resource;
import org.springframework.util.ObjectUtils;

/**
 * Base class exposing setters and handling the so-called Hadoop Generic options (files/libjars/archives) properties. 
 * 
 * @author Costin Leau
 */
abstract class JobGenericOptions {

	final Log log = LogFactory.getLog(getClass());

	Resource[] files, libJars, archives;
	String user;


	/**
	 * Sets the jar files to include in the classpath. 
	 * Note that a pattern can be used (e.g. <code>mydir/*.jar</code>), which the
	 * Spring container will automatically resolve.
	 * 
	 * @param libJars The jar files to include in the classpath.
	 */
	public void setLibs(Resource... libJars) {
		this.libJars = libJars;
	}

	/**
	 * Sets the files to be copied to the map reduce cluster. 
	 * Note that a pattern can be used (e.g. <code>mydir/*.txt</code>), which the
	 * Spring container will automatically resolve.
	 * 
	 * @param files The files to copy.
	 */
	public void setFiles(Resource... files) {
		this.files = files;
	}

	/**
	 * Sets the archives to be unarchive to the map reduce cluster. 
	 * Note that a pattern can be used (e.g. <code>mydir/*.zip</code>), which the
	 * Spring container will automatically resolve.
	 * 
	 * @param archives The archives to unarchive on the compute machines.
	 */
	public void setArchives(Resource... archives) {
		this.archives = archives;
	}


	void buildGenericOptions(Configuration cfg) {
		List<String> args = new ArrayList<String>();

		// populate config object
		try {
			// add known arguments first
			addResource(files, "-files", args);
			addResource(libJars, "-libjars", args);
			addResource(archives, "-archives", args);

			// set the GenericOptions properties manual to avoid the changes between Hadoop 1.x and 2.x
			cfg.setBoolean("mapred.used.genericoptionsparser", true);

			new GenericOptionsParser(cfg, args.toArray(new String[args.size()]));
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
	}

	void addResource(Resource[] args, String name, List<String> list) throws IOException {
		if (!ObjectUtils.isEmpty(args)) {
			int count = args.length;
			list.add(name);

			StringBuilder sb = new StringBuilder();
			for (Resource res : args) {
				sb.append(res.getURI().toString());
				if (--count > 0) {
					sb.append(",");
				}
			}
			list.add(sb.toString());

			if (log.isTraceEnabled()) {
				log.trace("Adding to generic option arg [" + name + "], resources " + sb.toString());
			}
		}
	}


	/**
	 * Sets the user impersonation (optional) for running this job.
	 * Should be used when running against a Hadoop Kerberos cluster. 
	 * 
	 * @param user user/group information
	 */
	public void setUser(String user) {
		this.user = user;
	}
}