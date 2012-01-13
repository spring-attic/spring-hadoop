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
package org.springframework.data.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.fs.PrettyPrintList.Printer;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ObjectUtils;

/**
 * HDFS FileSystem Shell supporting the 'hadoop fs/dfs [x]' commands as methods. 
 * See the <a href="http://hadoop.apache.org/common/docs/stable/file_system_shell.html">official guide</a> for more information.
 * <p/>
 * This class mimics as much as possible the shell behavior yet it is meant to be used in a programmatic way, 
 * that is rather then printing out information, they return object or collections that one can iterate through. If the message is
 * all that's needed then simply call the returned object {@link #toString()} explicitly or implicitly (by printing out or doing string
 * concatenation). 
 * 
 * @author Costin Leau
 */
public class FsShell {

	private FileSystem fs;
	private Configuration configuration;

	private abstract class FileBatch {
		abstract void process(Path p, FileSystem fs) throws IOException;

		final void run(Path src) {
			List<IOException> exceptions = new ArrayList<IOException>();

			try {
				// get filesystem
				FileSystem srcFs = src.getFileSystem(configuration);

				for (Path p : FileUtil.stat2Paths(fs.globStatus(src), src))
					try {
						process(p, srcFs);
					} catch (IOException ioe) {
						exceptions.add(ioe);
					}
			} catch (IOException ex) {
				throw new IllegalArgumentException("Cannot get fs", ex);
			}

			if (!exceptions.isEmpty())
				if (exceptions.size() == 1)
					throw new RuntimeException("Exception occurred", exceptions.get(0));
				else
					throw new RuntimeException("Multiple exceptions occurred: " + exceptions);
		}
	}

	private String getContent(InputStream in) throws IOException {
		StringWriter writer = new StringWriter(in.available());
		InputStreamReader reader = new InputStreamReader(in, "UTF-8");

		FileCopyUtils.copy(reader, writer);
		return writer.toString();
	}

	public Collection<Path> cat(String... uris) {
		if (ObjectUtils.isEmpty(uris)) {
			return Collections.emptyList();
		}


		final Collection<Path> results = new PrettyPrintList<Path>(new ArrayList<Path>(uris.length),
				new Printer<Path>() {

					@Override
					public String toString(Path e) throws IOException {
						return getContent(fs.open(e));
					}
				});

		try {

			for (String uri : uris) {
				Path src = new Path(uri);
				results.addAll(Arrays.asList(FileUtil.stat2Paths(fs.globStatus(src), src)));
			}
		} catch (IOException ex) {
			throw new IllegalArgumentException("Cannot execute command", ex);
		}

		return results;
	}

	public void chgrp(String group, String... uris) {
		chgrp(false, group, uris);
	}

	public void chgrpr(String group, String... uris) {
		chgrp(true, group, uris);
	}

	public void chgrp(boolean recursive, String group, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHGRP, recursive, group, uris);
	}

	public void chmod(String mode, String... uris) {
		chmod(false, mode, uris);
	}

	public void chmodr(String mode, String... uris) {
		chmod(true, mode, uris);
	}

	public void chmod(boolean recursive, String mode, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHMOD, recursive, mode, uris);
	}

	public void chown(String mode, String... uris) {
		chown(false, mode, uris);
	}

	public void chownr(String mode, String... uris) {
		chown(true, mode, uris);
	}

	public void chown(boolean recursive, String owner, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHOWN, recursive, owner, uris);
	}

	public void copyFromLocal(String src, String dst) {
		throw new UnsupportedOperationException();
	}

	public void copyToLocal(String dst, String src) {
		throw new UnsupportedOperationException();
	}

	public void copyToLocal(boolean ignorecrc, boolean crc, String dst, String src) {
		throw new UnsupportedOperationException();
	}

	public String count(String... uris) {
		throw new UnsupportedOperationException();
	}

	public String count(boolean quota, String... uris) {
		throw new UnsupportedOperationException();
	}

	public void cp(String src, String dst) {
		throw new UnsupportedOperationException();
	}

	public void cp(String src1, String src2, String... uris) {
		throw new UnsupportedOperationException();
	}

	public long du(String... uris) {
		throw new UnsupportedOperationException();
	}

	public Object du(boolean summary, boolean humanReadable, String... strings) {
		throw new UnsupportedOperationException();
	}

	public String dus(String... strings) {
		return (String) du(true, true, strings);
	}

	public void expunge() {
		throw new UnsupportedOperationException();
	}

	public void get(String dst, String src) {
		throw new UnsupportedOperationException();
	}

	public void get(boolean ignorecrc, boolean crc, String dst, String src) {
		throw new UnsupportedOperationException();
	}

	public void getmerge(String src, String localdst) {
		getmerge(src, localdst, false);
	}

	public void getmerge(String src, String localdst, boolean addnl) {
		throw new UnsupportedOperationException();
	}

	public String ls(String... args) {
		throw new UnsupportedOperationException();
	}

	public String lsr(String... args) {
		throw new UnsupportedOperationException();
	}

	public void mkdir(String... uris) {
		throw new UnsupportedOperationException();
	}

	public void moveFromLocal(String localsrc, String dst) {
		throw new UnsupportedOperationException();
	}

	public void moveToLocal(String src, String dst) {
		moveToLocal(false, src, dst);
	}

	public void moveToLocal(boolean crc, String src, String dst) {
		throw new UnsupportedOperationException();
	}

	public void mv(String src, String dst) {
		mv(src, null, dst);
	}

	public void mv(String src, String src2, String... dst) {
		throw new UnsupportedOperationException();
	}

	public void put(String localsrc, String dst) {
		throw new UnsupportedOperationException();
	}

	public void put(String localsrc, String localsrc2, String... dst) {
		throw new UnsupportedOperationException();
	}

	public void rm(String... uris) {
		rm(false, uris);
	}

	public void rm(boolean skipTrash, String... uris) {
		throw new UnsupportedOperationException();
	}

	public void rmr(String... uris) {
		rm(false, uris);
	}

	public void rmr(boolean skipTrash, String... uris) {
		throw new UnsupportedOperationException();
	}

	public int setrep(String uri) {
		return setrep(false, uri).iterator().next();
	}

	public Collection<Integer> setrep(boolean recursive, String uri) {
		throw new UnsupportedOperationException();
	}

	public Boolean test(String uri) {
		return test(false, false, false, uri);
	}

	public Boolean test(boolean exists, boolean zero, boolean directory, String uri) {
		throw new UnsupportedOperationException();
	}

	public String text(String uri) {
		throw new UnsupportedOperationException();
	}

	public void touchz(String... uris) {
		throw new UnsupportedOperationException();
	}

	/**
	 * @param fs The fs to set.
	 */
	public void setFs(FileSystem fs) {
		this.fs = fs;
	}

	/**
	 * @param configuration The configuration to set.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
}