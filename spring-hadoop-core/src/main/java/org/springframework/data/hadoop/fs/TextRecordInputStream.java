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

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Copy of FsShell$TextRecordInputStream allowing instantiation.
 *
 * @author Hadoop FsShell's authors
 * @author Costin Leau
 */
class TextRecordInputStream extends InputStream {

	SequenceFile.Reader r;
	WritableComparable<?> key;
	Writable val;

	DataInputBuffer inbuf;
	DataOutputBuffer outbuf;

	@SuppressWarnings("deprecation")
	public TextRecordInputStream(Path p, FileSystem fs, Configuration configuration) throws IOException {
		r = new SequenceFile.Reader(fs, p, configuration);
		key = ReflectionUtils.newInstance(r.getKeyClass().asSubclass(WritableComparable.class), configuration);
		val = ReflectionUtils.newInstance(r.getValueClass().asSubclass(Writable.class), configuration);
		inbuf = new DataInputBuffer();
		outbuf = new DataOutputBuffer();
	}

	public int read() throws IOException {
		int ret;
		if (null == inbuf || -1 == (ret = inbuf.read())) {
			if (!r.next(key, val)) {
				return -1;
			}
			byte[] tmp = key.toString().getBytes();
			outbuf.write(tmp, 0, tmp.length);
			outbuf.write('\t');
			tmp = val.toString().getBytes();
			outbuf.write(tmp, 0, tmp.length);
			outbuf.write('\n');
			inbuf.reset(outbuf.getData(), outbuf.getLength());
			outbuf.reset();
			ret = inbuf.read();
		}
		return ret;
	}


	public void close() throws IOException {
		r.close();
		super.close();
	}
}