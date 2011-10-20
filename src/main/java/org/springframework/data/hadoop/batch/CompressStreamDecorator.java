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
package org.springframework.data.hadoop.batch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.springframework.data.hadoop.batch.ResourcesItemWriter.InputStreamDecorator;
import org.springframework.data.hadoop.batch.ResourcesItemWriter.OutputStreamDecorator;

/**
 * Compressor/Decompressor processor based on {@link org.apache.hadoop.io.compress.CompressionCodec} API.
 * Allows compressed/uncompressed items to be decompressed/compressed after being read. 
 * 
 * @author Costin Leau
 */
public class CompressStreamDecorator implements InputStreamDecorator, OutputStreamDecorator {

	private CompressionCodec codec;

	public CompressStreamDecorator(CompressionCodec codec) {
		this.codec = codec;
	}

	public OutputStream decorate(OutputStream out) throws IOException {
		return codec.createOutputStream(out);
	}

	public InputStream decorate(InputStream in) throws IOException {
		return codec.createInputStream(in);
	}
}