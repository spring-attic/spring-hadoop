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
package org.springframework.data.hadoop.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * FileSystem decorator that overloads methods to accept Strings instead of {@link Path}s.
 * 
 * @author Costin Leau
 */
@SuppressWarnings("deprecation")
public class SimplerFileSystem extends FileSystem {

	private final FileSystem fs;

	/**
	 * Constructs a new <code>SimplerFileSystem</code> instance.
	 *
	 * @param fs Hadoop file system to use.
	 */
	public SimplerFileSystem(FileSystem fs) {
		Assert.notNull(fs, "fs is required");
		this.fs = fs;
	}

	public void setConf(Configuration conf) {
		if (fs != null) {
			fs.setConf(conf);
		}
	}

	public Configuration getConf() {
		return fs.getConf();
	}

	public int hashCode() {
		return fs.hashCode();
	}

	public boolean equals(Object obj) {
		return fs.equals(obj);
	}

	public void initialize(URI name, Configuration conf) throws IOException {
		fs.initialize(name, conf);
	}

	public URI getUri() {
		return fs.getUri();
	}

	public String getCanonicalServiceName() {
		return fs.getCanonicalServiceName();
	}

	public String getName() {
		return fs.getName();
	}

	public String toString() {
		return fs.toString();
	}

	public Path makeQualified(String path) {
		return fs.makeQualified(path(path));
	}

	public Path makeQualified(Path path) {
		return fs.makeQualified(path);
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return fs.getFileBlockLocations(file, start, len);
	}

	public FSDataInputStream open(String f, int bufferSize) throws IOException {
		return fs.open(path(f), bufferSize);
	}

	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		return fs.open(f, bufferSize);
	}

	public FSDataInputStream open(String f) throws IOException {
		return fs.open(path(f));
	}

	public FSDataInputStream open(Path f) throws IOException {
		return fs.open(f);
	}

	public FSDataOutputStream create(String f) throws IOException {
		return fs.create(path(f));
	}

	public FSDataOutputStream create(Path f) throws IOException {
		return fs.create(f);
	}

	public FSDataOutputStream create(String f, boolean overwrite) throws IOException {
		return fs.create(path(f), overwrite);
	}

	public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
		return fs.create(f, overwrite);
	}

	public FSDataOutputStream create(String f, Progressable progress) throws IOException {
		return fs.create(path(f), progress);
	}

	public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
		return fs.create(f, progress);
	}

	public FSDataOutputStream create(String f, short replication) throws IOException {
		return fs.create(path(f), replication);
	}

	public FSDataOutputStream create(Path f, short replication) throws IOException {
		return fs.create(f, replication);
	}

	public FSDataOutputStream create(String f, short replication, Progressable progress) throws IOException {
		return fs.create(path(f), replication, progress);
	}

	public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
		return fs.create(f, replication, progress);
	}

	public FSDataOutputStream create(String f, boolean overwrite, int bufferSize) throws IOException {
		return fs.create(path(f), overwrite, bufferSize);
	}

	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
		return fs.create(f, overwrite, bufferSize);
	}

	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
			throws IOException {
		return fs.create(f, overwrite, bufferSize, progress);
	}

	public FSDataOutputStream create(String f, boolean overwrite, int bufferSize, Progressable progress)
			throws IOException {
		return fs.create(path(f), overwrite, bufferSize, progress);
	}

	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {
		return fs.create(f, overwrite, bufferSize, replication, blockSize);
	}

	public FSDataOutputStream create(String f, boolean overwrite, int bufferSize, short replication, long blockSize)
			throws IOException {
		return fs.create(path(f), overwrite, bufferSize, replication, blockSize);
	}

	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		return fs.create(f, overwrite, bufferSize, replication, blockSize, progress);
	}

	public FSDataOutputStream create(String f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		return fs.create(path(f), overwrite, bufferSize, replication, blockSize, progress);
	}

	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		return fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
	}

	public FSDataOutputStream create(String f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		return fs.create(path(f), permission, overwrite, bufferSize, replication, blockSize, progress);
	}

	public boolean createNewFile(String f) throws IOException {
		return fs.createNewFile(path(f));
	}

	public boolean createNewFile(Path f) throws IOException {
		return fs.createNewFile(f);
	}

	public FSDataOutputStream append(String f) throws IOException {
		return fs.append(path(f));
	}

	public FSDataOutputStream append(Path f) throws IOException {
		return fs.append(f);
	}

	public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
		return fs.append(f, bufferSize);
	}

	public FSDataOutputStream append(String f, int bufferSize) throws IOException {
		return fs.append(path(f), bufferSize);
	}

	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		return fs.append(f, bufferSize, progress);
	}

	public FSDataOutputStream append(String f, int bufferSize, Progressable progress) throws IOException {
		return fs.append(path(f), bufferSize, progress);
	}

	public short getReplication(Path src) throws IOException {
		return fs.getReplication(src);
	}

	public short getReplication(String src) throws IOException {
		return fs.getReplication(path(src));
	}

	public boolean setReplication(Path src, short replication) throws IOException {
		return fs.setReplication(src, replication);
	}

	public boolean setReplication(String src, short replication) throws IOException {
		return fs.setReplication(path(src), replication);
	}

	public boolean rename(Path src, Path dst) throws IOException {
		return fs.rename(src, dst);
	}

	public boolean rename(String src, String dst) throws IOException {
		return fs.rename(path(src), path(dst));
	}

	public boolean delete(Path f) throws IOException {
		return fs.delete(f);
	}

	public boolean delete(String f) throws IOException {
		return fs.delete(path(f));
	}

	public boolean delete(Path f, boolean recursive) throws IOException {
		return fs.delete(f, recursive);
	}

	public boolean delete(String f, boolean recursive) throws IOException {
		return fs.delete(path(f), recursive);
	}

	public boolean deleteOnExit(Path f) throws IOException {
		return fs.deleteOnExit(f);
	}

	public boolean deleteOnExit(String f) throws IOException {
		return fs.deleteOnExit(path(f));
	}

	public boolean exists(Path f) throws IOException {
		return fs.exists(f);
	}

	public boolean exists(String f) throws IOException {
		return fs.exists(path(f));
	}

	public boolean isDirectory(Path f) throws IOException {
		return fs.isDirectory(f);
	}

	public boolean isDirectory(String f) throws IOException {
		return fs.isDirectory(path(f));
	}

	public boolean isFile(Path f) throws IOException {
		return fs.isFile(f);
	}

	public boolean isFile(String f) throws IOException {
		return fs.isFile(path(f));
	}

	public long getLength(Path f) throws IOException {
		return fs.getLength(f);
	}

	public long getLength(String f) throws IOException {
		return fs.getLength(path(f));
	}

	public ContentSummary getContentSummary(Path f) throws IOException {
		return fs.getContentSummary(f);
	}

	public ContentSummary getContentSummary(String f) throws IOException {
		return fs.getContentSummary(path(f));
	}

	public FileStatus[] listStatus(Path f) throws IOException {
		return fs.listStatus(f);
	}

	public FileStatus[] listStatus(String f) throws IOException {
		return fs.listStatus(path(f));
	}

	public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
		return fs.listStatus(f, filter);
	}

	public FileStatus[] listStatus(String f, PathFilter filter) throws IOException {
		return fs.listStatus(path(f), filter);
	}

	public FileStatus[] listStatus(Path[] files) throws IOException {
		return fs.listStatus(files);
	}

	public FileStatus[] listStatus(String[] files) throws IOException {
		return fs.listStatus(path(files));
	}

	public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
		return fs.listStatus(files, filter);
	}

	public FileStatus[] listStatus(String[] files, PathFilter filter) throws IOException {
		return fs.listStatus(path(files), filter);
	}

	public FileStatus[] globStatus(Path pathPattern) throws IOException {
		return fs.globStatus(pathPattern);
	}

	public FileStatus[] globStatus(String pathPattern) throws IOException {
		return fs.globStatus(path(pathPattern));
	}

	public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
		return fs.globStatus(pathPattern, filter);
	}

	public FileStatus[] globStatus(String pathPattern, PathFilter filter) throws IOException {
		return fs.globStatus(path(pathPattern), filter);
	}

	public Path getHomeDirectory() {
		return fs.getHomeDirectory();
	}

	public Token<?> getDelegationToken(String renewer) throws IOException {
		return fs.getDelegationToken(renewer);
	}

	public void setWorkingDirectory(Path new_dir) {
		fs.setWorkingDirectory(new_dir);
	}

	public Path getWorkingDirectory() {
		return fs.getWorkingDirectory();
	}

	public boolean mkdirs(Path f) throws IOException {
		return fs.mkdirs(f);
	}

	public boolean mkdirs(String f) throws IOException {
		return fs.mkdirs(path(f));
	}

	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		return fs.mkdirs(f, permission);
	}

	public boolean mkdirs(String f, short permission) throws IOException {
		return fs.mkdirs(path(f), new FsPermission(permission));
	}

	public void copyFromLocalFile(Path src, Path dst) throws IOException {
		fs.copyFromLocalFile(src, dst);
	}

	public void copyFromLocalFile(String src, String dst) throws IOException {
		fs.copyFromLocalFile(path(src), path(dst));
	}

	public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
		fs.moveFromLocalFile(srcs, dst);
	}

	public void moveFromLocalFile(String[] srcs, String dst) throws IOException {
		fs.moveFromLocalFile(path(srcs), path(dst));
	}

	public void moveFromLocalFile(Path src, Path dst) throws IOException {
		fs.moveFromLocalFile(src, dst);
	}

	public void moveFromLocalFile(String src, String dst) throws IOException {
		fs.moveFromLocalFile(path(src), path(dst));
	}

	public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
		fs.copyFromLocalFile(delSrc, src, dst);
	}

	public void copyFromLocalFile(boolean delSrc, String src, String dst) throws IOException {
		fs.copyFromLocalFile(delSrc, path(src), path(dst));
	}

	public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
		fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
	}


	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String[] srcs, String dst) throws IOException {
		fs.copyFromLocalFile(delSrc, overwrite, path(srcs), path(dst));
	}

	public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
		fs.copyFromLocalFile(delSrc, overwrite, src, dst);
	}

	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String src, String dst) throws IOException {
		fs.copyFromLocalFile(delSrc, overwrite, path(src), path(dst));
	}

	public void copyToLocalFile(Path src, Path dst) throws IOException {
		fs.copyToLocalFile(src, dst);
	}

	public void copyToLocalFile(String src, String dst) throws IOException {
		fs.copyToLocalFile(path(src), path(dst));
	}

	public void moveToLocalFile(Path src, Path dst) throws IOException {
		fs.moveToLocalFile(src, dst);
	}

	public void moveToLocalFile(String src, String dst) throws IOException {
		fs.moveToLocalFile(path(src), path(dst));
	}

	public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
		fs.copyToLocalFile(delSrc, src, dst);
	}

	public void copyToLocalFile(boolean delSrc, String src, String dst) throws IOException {
		fs.copyToLocalFile(delSrc, path(src), path(dst));
	}

	public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
		return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
	}

	public Path startLocalOutput(String fsOutputFile, String tmpLocalFile) throws IOException {
		return fs.startLocalOutput(path(fsOutputFile), path(tmpLocalFile));
	}

	public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
		fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
	}

	public void completeLocalOutput(String fsOutputFile, String tmpLocalFile) throws IOException {
		fs.completeLocalOutput(path(fsOutputFile), path(tmpLocalFile));
	}

	public void close() throws IOException {
		fs.close();
	}

	public long getUsed() throws IOException {
		return fs.getUsed();
	}

	public long getBlockSize(Path f) throws IOException {
		return fs.getBlockSize(f);
	}

	public long getBlockSize(String f) throws IOException {
		return fs.getBlockSize(path(f));
	}

	public long getDefaultBlockSize() {
		return fs.getDefaultBlockSize();
	}

	public short getDefaultReplication() {
		return fs.getDefaultReplication();
	}

	public FileStatus getFileStatus(Path f) throws IOException {
		return fs.getFileStatus(f);
	}

	public FileStatus getFileStatus(String f) throws IOException {
		return fs.getFileStatus(path(f));
	}

	public FileChecksum getFileChecksum(Path f) throws IOException {
		return fs.getFileChecksum(f);
	}

	public FileChecksum getFileChecksum(String f) throws IOException {
		return fs.getFileChecksum(path(f));
	}

	public void setVerifyChecksum(boolean verifyChecksum) {
		fs.setVerifyChecksum(verifyChecksum);
	}

	public void setPermission(Path p, FsPermission permission) throws IOException {
		fs.setPermission(p, permission);
	}

	public void setPermission(String p, short permission) throws IOException {
		fs.setPermission(path(p), new FsPermission(permission));
	}

	public void setOwner(Path p, String username, String groupname) throws IOException {
		fs.setOwner(p, username, groupname);
	}

	public void setOwner(String p, String username, String groupname) throws IOException {
		fs.setOwner(path(p), username, groupname);
	}

	public void setTimes(Path p, long mtime, long atime) throws IOException {
		fs.setTimes(p, mtime, atime);
	}

	public void setTimes(String p, long mtime, long atime) throws IOException {
		fs.setTimes(path(p), mtime, atime);
	}

	private Path path(String path) {
		return new Path(path);
	}

	private Path[] path(String[] files) {
		Path[] paths = null;
		if (!ObjectUtils.isEmpty(files)) {
			paths = new Path[files.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = path(files[i]);
			}
		}
		else {
			paths = new Path[0];
		}
		return paths;
	}
}