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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.data.hadoop.fs.PrettyPrintList.ListPrinter;
import org.springframework.data.hadoop.fs.PrettyPrintMap.MapPrinter;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * HDFS FileSystem Shell supporting the 'hadoop fs/dfs [x]' commands as methods.
 * See the <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html">official guide</a> for more information.
 * <p>
 * This class mimics as much as possible the shell behavior yet it is meant to be used in a programmatic way,
 * that is rather then printing out information, they return object or collections that one can iterate through. If the message is
 * all that's needed then simply call the returned object {@link #toString()} explicitly or implicitly (by printing out or doing string
 * concatenation).
 *
 * @author Hadoop's FsShell authors
 * @author Costin Leau
 */
@SuppressWarnings("deprecation")
public class FsShell implements Closeable, DisposableBean {

	private boolean internalFs = false;
	private FileSystem fs;
	private final Configuration configuration;
	private Trash trash;


	/**
	 * Constructs a new <code>FsShell</code> instance.
	 *
	 * @param configuration Hadoop configuration to use.
	 */
	public FsShell(Configuration configuration) {
		this(configuration, null);
	}

	/**
	 * Constructs a new <code>FsShell</code> instance.
	 *
	 * @param configuration Hadoop configuration to use.
	 * @param fs Hadoop file system to use.
	 */
	public FsShell(Configuration configuration, FileSystem fs) {
		this.configuration = configuration;
		try {
			this.fs = (fs != null ? fs : FileSystem.get(configuration));
			this.internalFs = (fs == null);
			this.trash = new Trash(configuration);
		} catch (IOException ex) {
			throw new HadoopException("Cannot create shell " + ex.getMessage(), ex);
		}
	}

	@Override
	public void destroy() throws Exception {
		close();
	}

	@Override
	public void close() throws IOException {
		if (internalFs && fs != null) {
			fs.close();
			fs = null;
		}
	}

	private String getContent(InputStream in) throws IOException {
		StringWriter writer = new StringWriter(in.available());
		InputStreamReader reader = new InputStreamReader(in, "UTF-8");

		FileCopyUtils.copy(reader, writer);
		return writer.toString();
	}

	public Collection<Path> cat(String uri) {
		return cat(new String[] { uri });
	}

	public Collection<Path> cat(String... uris) {
		final Collection<Path> results = new PrettyPrintList<Path>(new ListPrinter<Path>() {
			@Override
			public String toString(Path e) throws IOException {
				try {
					final FileSystem srcFS = getFS(e);
					return getContent(srcFS.open(e));
				} catch (IOException ex) {
					return "No such file or directory " + e.toUri();
				}
			}
		});

		try {
			if (!ObjectUtils.isEmpty(uris)) {
				for (String uri : uris) {
					Path src = new Path(uri);
					FileSystem srcFS = getFS(src);
					results.addAll(Arrays.asList(FileUtil.stat2Paths(srcFS.globStatus(src), src)));
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot execute command " + ex.getMessage(), ex);
		}

		return Collections.unmodifiableCollection(results);
	}

	public void chgrp(String group, String uri) {
		chgrp(group, new String[] { uri });
	}

	public void chgrp(String group, String... uris) {
		chgrp(false, group, uris);
	}

	public void chgrpr(String group, String uri) {
		chgrpr(group, new String[] { uri });
	}

	public void chgrpr(String group, String... uris) {
		chgrp(true, group, uris);
	}

	public void chgrp(boolean recursive, String group, String uri) {
		chgrp(recursive, group, new String[] { uri });
	}

	public void chgrp(boolean recursive, String group, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHGRP, recursive, group, uris);
	}

	public void chmod(String mode, String uri) {
		chmod(mode, new String[] { uri });
	}

	public void chmod(String mode, String... uris) {
		chmod(false, mode, uris);
	}

	public void chmodr(String mode, String uri) {
		chmodr(mode, new String[] { uri });
	}

	public void chmodr(String mode, String... uris) {
		chmod(true, mode, uris);
	}

	public void chmodr(Integer mode, String uri) {
		chmodr(mode, new String[] { uri });
	}

	public void chmodr(Integer mode, String... uris) {
		chmod(true, String.valueOf(mode), uris);
	}

	public void chmod(Integer mode, String uri) {
		chmod(mode, new String[] { uri });
	}

	public void chmod(Integer mode, String... uris) {
		chmod(false, String.valueOf(mode), uris);
	}

	public void chmod(boolean recursive, Integer mode, String uri) {
		chmod(recursive, mode, new String[] { uri });
	}

	public void chmod(boolean recursive, Integer mode, String... uris) {
		chmod(recursive, String.valueOf(mode), uris);
	}

	public void chmod(boolean recursive, String mode, String uri) {
		chmod(recursive, mode, new String[] { uri });
	}

	public void chmod(boolean recursive, String mode, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHMOD, recursive, mode, uris);
	}

	public void chown(String mode, String uri) {
		chown(mode, new String[] { uri });
	}

	public void chown(String mode, String... uris) {
		chown(false, mode, uris);
	}

	public void chownr(String mode, String uri) {
		chownr(mode, new String[] { uri });
	}

	public void chownr(String mode, String... uris) {
		chown(true, mode, uris);
	}

	public void chown(boolean recursive, String owner, String uri) {
		chown(recursive, owner, new String[] { uri });
	}

	public void chown(boolean recursive, String owner, String... uris) {
		FsShellPermissions.changePermissions(fs, configuration, FsShellPermissions.Op.CHOWN, recursive, owner, uris);
	}

	public void copyFromLocal(String src, String dst) {
		copyFromLocal(src, dst, (String[]) null);
	}

	public void copyFromLocal(String src, String src2, String... dst) {
		Object[] va = parseVarargs(src, src2, dst);
		@SuppressWarnings("unchecked")
		List<Path> srcs = (List<Path>) va[0];
		Path dstPath = (Path) va[1];

		try {
			FileSystem dstFs = getFS(dstPath);
			dstFs.copyFromLocalFile(false, false, srcs.toArray(new Path[srcs.size()]), dstPath);
		} catch (IOException ex) {
			throw new HadoopException("Cannot copy resources " + ex.getMessage(), ex);
		}
	}

	public void copyToLocal(String src, String localdst) {
		copyToLocal(true, false, src, localdst);
	}

	public void copyToLocal(boolean ignorecrc, boolean crc, String src, String localdst) {
		File dst = new File(localdst);
		Path srcpath = new Path(src);

		try {
			FileSystem srcFs = getFS(srcpath);
			srcFs.setVerifyChecksum(ignorecrc);
			if (crc && !(srcFs instanceof ChecksumFileSystem)) {
				crc = false;
			}
			FileStatus[] srcs = srcFs.globStatus(srcpath);
			boolean dstIsDir = dst.isDirectory();
			if (srcs.length > 1 && !dstIsDir) {
				throw new IllegalArgumentException("When copying multiple files, "
						+ "destination should be a directory.");
			}
			for (FileStatus status : srcs) {
				Path p = status.getPath();
				File f = dstIsDir ? new File(dst, p.getName()) : dst;
				copyToLocal(srcFs, p, f, crc);
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot copy resources " + ex.getMessage(), ex);
		}
	}

	// copied from FsShell
	private void copyToLocal(final FileSystem srcFS, final Path src, final File dst, final boolean copyCrc)
			throws IOException {

		final String COPYTOLOCAL_PREFIX = "_copyToLocal_";

		/* Keep the structure similar to ChecksumFileSystem.copyToLocal().
		* Ideal these two should just invoke FileUtil.copy() and not repeat
		* recursion here. Of course, copy() should support two more options :
		* copyCrc and useTmpFile (may be useTmpFile need not be an option).
		*/
		if (!srcFS.getFileStatus(src).isDir()) {
			if (dst.exists()) {
				// match the error message in FileUtil.checkDest():
				throw new IOException("Target " + dst + " already exists");
			}

			// use absolute name so that tmp file is always created under dest dir
			File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(), COPYTOLOCAL_PREFIX, true);
			if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
				throw new IOException("Failed to copy " + src + " to " + dst);
			}

			if (!tmp.renameTo(dst)) {
				throw new IOException("Failed to rename tmp file " + tmp + " to local destination \"" + dst + "\".");
			}

			if (copyCrc) {
				if (!(srcFS instanceof ChecksumFileSystem)) {
					throw new IOException("Source file system does not have crc files");
				}

				ChecksumFileSystem csfs = (ChecksumFileSystem) srcFS;
				File dstcs = FileSystem.getLocal(srcFS.getConf()).pathToFile(
						csfs.getChecksumFile(new Path(dst.getCanonicalPath())));
				copyToLocal(csfs.getRawFileSystem(), csfs.getChecksumFile(src), dstcs, false);
			}
		}
		else {
			// once FileUtil.copy() supports tmp file, we don't need to mkdirs().
			dst.mkdirs();
			for (FileStatus path : srcFS.listStatus(src)) {
				copyToLocal(srcFS, path.getPath(), new File(dst, path.getPath().getName()), copyCrc);
			}
		}
	}

	public Map<Path, ContentSummary> count(String uri) {
		return count(new String[] { uri });
	}

	public Map<Path, ContentSummary> count(String... uris) {
		return count(false, uris);
	}

	public Map<Path, ContentSummary> count(final boolean quota, String uri) {
		return count(quota, new String[] { uri });
	}

	public Map<Path, ContentSummary> count(final boolean quota, String... uris) {

		final Map<Path, ContentSummary> results = new PrettyPrintMap<Path, ContentSummary>(uris.length,
				new MapPrinter<Path, ContentSummary>() {
					@Override
					public String toString(Path p, ContentSummary c) throws IOException {
						return c.toString(quota) + p;
					}
				});

		for (String src : uris) {
			try {
				Path srcPath = new Path(src);
				final FileSystem fs = getFS(srcPath);
				FileStatus[] statuses = fs.globStatus(srcPath);
				Assert.notEmpty(statuses, "Can not find listing for " + src);
				for (FileStatus s : statuses) {
					Path p = s.getPath();
					results.put(p, fs.getContentSummary(p));
				}
			} catch (IOException ex) {
				throw new HadoopException("Cannot find listing " + ex.getMessage(), ex);
			}
		}

		return Collections.unmodifiableMap(results);
	}

	public void cp(String src, String dst) {
		cp(src, dst, (String[]) null);
	}

	public void cp(String src, String src2, String... dst) {
		Object[] va = parseVarargs(src, src2, dst);
		@SuppressWarnings("unchecked")
		List<Path> srcs = (List<Path>) va[0];
		Path dstPath = (Path) va[1];

		try {

			FileSystem dstFs = dstPath.getFileSystem(configuration);
			boolean isDestDir = !dstFs.isFile(dstPath);

			if (StringUtils.hasText(src2) || (ObjectUtils.isEmpty(dst) && dst.length > 2)) {
				if (!isDestDir) {
					throw new IllegalArgumentException("When copying multiple files, destination " + dstPath.toUri()
							+ " should be a directory.");
				}
			}

			for (Path path : srcs) {
				FileSystem srcFs = path.getFileSystem(configuration);
				Path[] from = FileUtil.stat2Paths(srcFs.globStatus(path), path);
				if (!ObjectUtils.isEmpty(from) && from.length > 1 && !isDestDir) {
					throw new IllegalArgumentException(
							"When copying multiple files, destination should be a directory.");
				}
				for (Path fromPath : from) {
					FileUtil.copy(srcFs, fromPath, dstFs, dstPath, false, configuration);
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot copy resources " + ex.getMessage(), ex);
		}
	}

	public Map<Path, Long> du(String uri) {
		return du(new String[] { uri });
	}

	public Map<Path, Long> du(String... uris) {
		return du(false, uris);
	}

	public Map<Path, Long> du(final boolean summary, String string) {
		return du(summary, new String[] { string });
	}

	public Map<Path, Long> du(final boolean summary, String... strings) {
		if (ObjectUtils.isEmpty(strings)) {
			strings = new String[] { "." };
		}

		final int BORDER = 2;

		Map<Path, Long> results = new PrettyPrintMap<Path, Long>(strings.length, new MapPrinter<Path, Long>() {

			@Override
			public String toString(Path path, Long size) throws Exception {
				if (summary) {
					return ("".equals(path) ? "." : path) + "\t" + size;
				}
				return String.format("%-" + (10 + BORDER) + "d", size) + path;
			}
		});

		try {
			for (String src : strings) {
				Path srcPath = new Path(src);
				FileSystem srcFs = getFS(srcPath);
				FileStatus[] fileStatus = srcFs.globStatus(srcPath);
				if (summary) {
					for (FileStatus status : fileStatus) {
						results.put(status.getPath(), srcFs.getContentSummary(status.getPath()).getLength());
					}
				}
				else {
					FileStatus items[] = srcFs.listStatus(FileUtil.stat2Paths(fileStatus, srcPath));
					if (ObjectUtils.isEmpty(items) && (!srcFs.exists(srcPath))) {
						throw new HadoopException("Cannot access " + src + ": No such file or directory.");
					}
					for (FileStatus status : items) {
						Long size = (status.isDir() ? srcFs.getContentSummary(status.getPath()).getLength() : status.getLen());
						results.put(status.getPath(), size);
					}
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot inspect resources " + ex.getMessage(), ex);
		}

		return Collections.unmodifiableMap(results);
	}

	public Map<Path, Long> dus(String string) {
		return dus(new String[] { string });
	}

	public Map<Path, Long> dus(String... strings) {
		return du(true, strings);
	}

	public void expunge() {
		try {
			trash.expunge();
			trash.checkpoint();
		} catch (IOException ex) {
			throw new HadoopException("Cannot expunge trash" + ex.getMessage(), ex);
		}
	}

	public void get(String src, String dst) {
		copyToLocal(src, dst);
	}

	public void get(boolean ignorecrc, boolean crc, String src, String dst) {
		copyToLocal(ignorecrc, crc, src, dst);
	}

	public void getmerge(String src, String localdst) {
		getmerge(src, localdst, false);
	}

	public void getmerge(String src, String localdst, boolean addnl) {
		Path srcPath = new Path(src);
		Path dst = new Path(localdst);
		try {
			FileSystem srcFs = getFS(srcPath);
			Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
			for (int i = 0; i < srcs.length; i++) {
				if (addnl) {
					FileUtil.copy(srcFs, srcs[i], FileSystem.getLocal(configuration), dst, false, configuration);
				}
				else {
					FileUtil.copy(srcFs, srcs[i], FileSystem.getLocal(configuration), dst, false, configuration);
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot getmerge " + ex.getMessage(), ex);
		}
	}

	public Collection<FileStatus> ls(String match) {
		return ls(false, new String[] { match });
	}

	public Collection<FileStatus> ls(String... match) {
		return ls(false, match);
	}

	public Collection<FileStatus> ls(boolean recursive, String match) {
		return ls(recursive, new String[] { match });
	}

	public Collection<FileStatus> ls(boolean recursive, String... match) {

		Collection<FileStatus> results = new PrettyPrintList<FileStatus>(new ListPrinter<FileStatus>() {
			@Override
			public String toString(FileStatus stat) throws Exception {
				final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				int maxReplication = 3, maxLen = 10, maxOwner = 10, maxGroup = 10;

				StringBuilder sb = new StringBuilder();
				sb.append((stat.isDir() ? "d" : "-") + stat.getPermission() + " ");
				sb.append(String.format("%" + maxReplication + "s ", (!stat.isDir() ? stat.getReplication() : "-")));
				sb.append(String.format("%-" + maxOwner + "s ", stat.getOwner()));
				sb.append(String.format("%-" + maxGroup + "s ", stat.getGroup()));
				sb.append(String.format("%" + maxLen + "d ", stat.getLen()));
				sb.append(df.format(new Date(stat.getModificationTime())) + " ");
				sb.append(stat.getPath().toUri().getPath());
				return sb.toString();
			}
		});

		try {
			for (String src : match) {
				Path srcPath = new Path(src);

				FileSystem srcFs = getFS(srcPath);
				FileStatus[] srcs = srcFs.globStatus(srcPath);
				if (!ObjectUtils.isEmpty(srcs)) {
					for (FileStatus status : srcs) {
						ls(status, srcFs, recursive, results);
					}
				}
				else {
					throw new IllegalArgumentException("Cannot access " + srcPath + ": No such file or directory.");
				}
			}

			return Collections.unmodifiableCollection(results);

		} catch (IOException ex) {
			throw new HadoopException("Cannot list resources " + ex.getMessage(), ex);
		}
	}

	private void ls(FileStatus src, FileSystem srcFs, boolean recursive, Collection<FileStatus> results)
			throws IOException {

		results.add(src);

		if (src.isDir()) {
			final FileStatus[] items = srcFs.listStatus(src.getPath());
			if (!ObjectUtils.isEmpty(items)) {
				for (FileStatus stat : items) {
					if (recursive && stat.isDir()) {
						ls(stat, srcFs, recursive, results);
					}
					else {
						results.add(stat);
					}
				}
			}
		}
	}


	public Collection<FileStatus> lsr(String match) {
		return ls(true, match);
	}

	public Collection<FileStatus> lsr(String... match) {
		return ls(true, match);
	}

	public void mkdir(String uri) {
		mkdir(new String[] { uri });
	}

	public void mkdir(String... uris) {
		for (String src : uris) {
			try {
				Path p = new Path(src);
				FileSystem srcFs = getFS(p);
				FileStatus fstatus = null;
				try {
					fstatus = srcFs.getFileStatus(p);
					if (fstatus.isDir()) {
						throw new IllegalStateException("Cannot create directory " + src + ": File exists");
					}
					else {
						throw new IllegalStateException(src + " exists but is not a directory");
					}
				} catch (FileNotFoundException e) {
					if (!srcFs.mkdirs(p)) {
						throw new HadoopException("Failed to create " + src);
					}
				}
			} catch (IOException ex) {
				throw new HadoopException("Cannot create directory " + ex.getMessage(), ex);
			}
		}
	}

	public void moveFromLocal(String localsrc, String dst) {
		moveFromLocal(localsrc, dst, (String[]) null);
	}

	public void moveFromLocal(String localsrc, String localsrc2, String... dst) {
		Object[] va = parseVarargs(localsrc, localsrc2, dst);
		@SuppressWarnings("unchecked")
		List<Path> srcs = (List<Path>) va[0];
		Path dstPath = (Path) va[1];

		try {
			FileSystem dstFs = dstPath.getFileSystem(configuration);
			dstFs.moveFromLocalFile(srcs.toArray(new Path[srcs.size()]), dstPath);
		} catch (IOException ex) {
			throw new HadoopException("Cannot move resources " + ex.getMessage(), ex);
		}
	}

	public void moveToLocal(String src, String dst) {
		moveToLocal(false, src, dst);
	}

	public void moveToLocal(boolean crc, String src, String dst) {
		throw new UnsupportedOperationException("Option 'moveToLocal' is not implemented yet.");
	}

	public void mv(String src, String dst) {
		mv(src, dst, (String[]) null);
	}

	public void mv(String src, String src2, String... dst) {
		Object[] va = parseVarargs(src, src2, dst);
		@SuppressWarnings({ "unchecked" })
		List<Path> sources = (List<Path>) va[0];
		Path dstPath = (Path) va[1];

		try {
			FileSystem dstFs = getFS(dstPath);
			boolean isDstDir = !dstFs.isFile(dstPath);

			if (sources.size() > 1 && !isDstDir) {
				throw new IllegalArgumentException("Destination must be a dir when moving multiple files");
			}

			for (Path srcPath : sources) {
				FileSystem srcFs = getFS(srcPath);
				URI srcURI = srcFs.getUri();
				URI dstURI = dstFs.getUri();
				if (srcURI.compareTo(dstURI) != 0) {
					throw new IllegalArgumentException("src and destination filesystems do not match.");
				}
				Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
				if (srcs.length > 1 && !isDstDir) {
					throw new IllegalArgumentException("When moving multiple files, destination should be a directory.");
				}
				for (Path s : srcs) {
					if (!srcFs.rename(s, dstPath)) {
						FileStatus srcFstatus = null;
						FileStatus dstFstatus = null;
						try {
							srcFstatus = srcFs.getFileStatus(s);
						} catch (FileNotFoundException e) {
							// ignore
						}
						try {
							dstFstatus = dstFs.getFileStatus(dstPath);
						} catch (IOException e) {
						}
						if ((srcFstatus != null) && (dstFstatus != null)) {
							if (srcFstatus.isDir() && !dstFstatus.isDir()) {
								throw new IllegalArgumentException("cannot overwrite non directory " + dstPath
										+ " with directory " + s);
							}
						}
						throw new HadoopException("Failed to rename " + s + " to " + dstPath);
					}
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot rename resources " + ex.getMessage(), ex);
		}
	}

	public void put(String localsrc, String dst) {
		put(localsrc, dst, (String[]) null);
	}

	public void put(String localsrc, String localsrc2, String... dst) {
		copyFromLocal(localsrc, localsrc2, dst);
	}

	public void rm(String uri) {
		rm(new String[] { uri });
	}

	public void rm(String... uris) {
		rm(false, false, uris);
	}

	public void rm(boolean recursive, String uri) {
		rm(recursive, new String[] { uri });
	}

	public void rm(boolean recursive, String... uris) {
		rm(recursive, false, uris);
	}

	public void rm(boolean recursive, boolean skipTrash, String... uris) {
		for (String uri : uris) {
			try {
				Path src = new Path(uri);
				FileSystem srcFs = getFS(src);

				for (Path p : FileUtil.stat2Paths(srcFs.globStatus(src), src)) {
					FileStatus status = srcFs.getFileStatus(p);
					if (status.isDir() && !recursive) {
						throw new IllegalStateException("Cannot remove directory \"" + src
								+ "\", if recursive deletion was not specified");
					}
					if (!skipTrash) {
						try {
							Trash trashTmp = new Trash(srcFs, configuration);
							trashTmp.moveToTrash(p);
						} catch (IOException ex) {
							throw new HadoopException("Cannot move to Trash resource " + p, ex);
						}
					}
					srcFs.delete(p, recursive);
				}
			} catch (IOException ex) {
				throw new HadoopException("Cannot delete (all) resources " + ex.getMessage(), ex);
			}
		}
	}

	public void rmr(String uri) {
		rmr(new String[] { uri });
	}

	public void rmr(String... uris) {
		rm(true, false, uris);
	}

	public void rmr(boolean skipTrash, String uri) {
		rmr(skipTrash, new String[] { uri });
	}

	public void rmr(boolean skipTrash, String... uris) {
		rm(true, skipTrash, uris);
	}

	public void setrep(short replication, String uri) {
		setrep(replication, new String[] { uri });
	}

	public void setrep(short replication, String... uris) {
		setrep(false, replication, uris);
	}

	public void setrep(boolean recursive, short replication, String uri) {
		setrep(recursive, replication, new String[] { uri });
	}

	public void setrep(boolean recursive, short replication, String... uris) {
		setrep(-1, recursive, replication, uris);
	}

	public void setrepr(short replication, String... uris) {
		setrep(-1, true, replication, uris);
	}

	public void setrepr(short replication, String uri) {
		setrepr(replication, new String[] { uri });
	}

	public void setrepr(long secondsToWait, short replication, String uri) {
		setrepr(secondsToWait, replication, new String[] { uri });
	}

	public void setrepr(long secondsToWait, short replication, String... uris) {
		setrep(secondsToWait, true, replication, uris);
	}

	public void setrep(long secondsToWait, boolean recursive, short replication, String uri) {
		setrep(secondsToWait, recursive, replication, new String[] { uri });
	}

	public void setrep(long secondsToWait, boolean recursive, short replication, String... uris) {
		Assert.isTrue(replication >= 1, "Replication must be >=1");

		List<Path> waitList = (secondsToWait >= 0 ? new ArrayList<Path>() : null);

		try {
			for (String uri : uris) {
				Path srcPath = new Path(uri);
				FileSystem srcFs = getFS(srcPath);
				Path[] srcs = FileUtil.stat2Paths(srcFs.globStatus(srcPath), srcPath);
				for (Path src : srcs) {
					setrep(replication, recursive, srcFs, src, waitList);
				}
			}

			if (waitList != null) {
				boolean waitUntilDone = (secondsToWait == 0);
				long timeLeft = TimeUnit.SECONDS.toMillis(secondsToWait);

				for (Path path : waitList) {
					FileSystem srcFs = getFS(path);
					FileStatus status = srcFs.getFileStatus(path);
					long len = status.getLen();

					boolean done = false;

					while (!done) {
						BlockLocation[] locations = srcFs.getFileBlockLocations(status, 0, len);
						int i = 0;
						for (; i < locations.length && locations[i].getHosts().length == replication; i++) {
						}
						done = (i == locations.length);

						if (!done && (waitUntilDone || timeLeft > 5000)) {
							try {
								// sleep for 10s
								Thread.sleep(10000);
							} catch (InterruptedException e) {
								return;
							}
							timeLeft = -1000;
						}
					}
				}
			}
		} catch (IOException ex) {
			throw new HadoopException("Cannot set replication " + ex.getMessage(), ex);
		}
	}

	private void setrep(short replication, boolean recursive, FileSystem srcFs, Path src, List<Path> waitList)
			throws IOException {
		if (srcFs.isFile(src)) {
			if (srcFs.setReplication(src, replication)) {
				if (waitList != null) {
					waitList.add(src);
				}
			}
			else {
				throw new HadoopException("Cannot set replication for " + src);
			}
		}
		else {
			if (recursive) {
				FileStatus items[] = srcFs.listStatus(src);
				if (!ObjectUtils.isEmpty(items)) {
					for (FileStatus status : items) {
						setrep(replication, recursive, srcFs, status.getPath(), waitList);
					}
				}
			}
		}
	}

	public boolean test(String uri) {
		return test(true, false, false, uri);
	}

	public boolean test(boolean exists, boolean zero, boolean directory, String uri) {
		Path f = new Path(uri);

		boolean result = true;
		try {
			FileSystem srcFs = getFS(f);
			// mandatory check - if this fails, so will the others (with a NPE most likely)
			result = srcFs.exists(f);

			if (result && zero) {
				result &= srcFs.getFileStatus(f).getLen() == 0;
			}
			if (result && directory) {
				result &= srcFs.getFileStatus(f).isDir();
			}

			return result;
		} catch (IOException ex) {
			throw new HadoopException("Cannot test resource " + uri + ";" + ex.getMessage(), ex);
		}
	}

	public Collection<String> text(String uri) {
		return text(new String[] { uri });
	}

	public Collection<String> text(String... uris) {
		Collection<String> texts = new PrettyPrintList<String>(new ListPrinter<String>() {

			@Override
			public String toString(String e) throws Exception {
				return e + "\n";
			}
		});

		for (String uri : uris) {

			InputStream in = null;
			FSDataInputStream i = null;

			try {
				Path srcPat = new Path(uri);
				FileSystem srcFs = getFS(srcPat);

				for (Path src : FileUtil.stat2Paths(srcFs.globStatus(srcPat), srcPat)) {
					Assert.isTrue(srcFs.isFile(src), "Source must be a file");
					i = srcFs.open(src);
					switch (i.readShort()) {
					case 0x1f8b: // RFC 1952
						i.seek(0);
						in = new GZIPInputStream(i);
						break;
					case 0x5345: // 'S' 'E'
						if (i.readByte() == 'Q') {
							i.close();
							in = new TextRecordInputStream(src, srcFs, configuration);
						}
						break;
					default:
						in = i;
						break;
					}
					i.seek(0);
					texts.add(getContent(in));
				}
			} catch (IOException ex) {
				throw new HadoopException("Cannot read " + uri + ";" + ex.getMessage(), ex);
			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(i);
			}
		}
		return texts;
	}

	public void touchz(String uri) {
		touchz(new String[] { uri });
	}

	public void touchz(String... uris) {
		for (String uri : uris) {
			try {
				Path src = new Path(uri);
				FileSystem srcFs = getFS(src);
				FileStatus st;
				if (srcFs.exists(src)) {
					st = srcFs.getFileStatus(src);
					if (st.isDir()) {
						// TODO: handle this
						throw new IllegalArgumentException(src + " is a directory");
					}
					else if (st.getLen() != 0)
						throw new IllegalArgumentException(src + " must be a zero-length file");
				}
				else {
					IOUtils.closeStream(srcFs.create(src));
				}
			} catch (IOException ex) {
				throw new HadoopException("Cannot touchz " + uri + ";" + ex.getMessage(), ex);
			}
		}
	}

	private static Object[] parseVarargs(String src1, String src2, String... dst) {
		Assert.hasText(src1, "at least one valid source path needs to be specified");

		Path dstPath = null;
		// create src path
		List<Path> srcs = new ArrayList<Path>();
		srcs.add(new Path(src1));


		if (!ObjectUtils.isEmpty(dst)) {
			srcs.add(new Path(src2));
			for (int i = 0; i < dst.length - 1; i++) {
				srcs.add(new Path(dst[i]));
			}
			dstPath = new Path(dst[dst.length - 1]);
		}
		else {
			dstPath = new Path(src2);
		}

		return new Object[] { srcs, dstPath };
	}

	/**
	 * Utility that checks whether the given path has a URI - if it doesn't, it falls back
	 * to the specified FS (rather then always HDFS as Hadoop does).
	 *
	 * @param path path
	 * @return associated file system
	 */
	private FileSystem getFS(Path path) throws IOException {
		if (StringUtils.hasText(path.toUri().getScheme())) {
			return path.getFileSystem(configuration);
		}
		return fs;
	}
}
