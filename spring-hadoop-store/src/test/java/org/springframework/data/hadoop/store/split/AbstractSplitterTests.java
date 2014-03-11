/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.hadoop.store.split;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Base class for all splitter tests having a shared
 * mocking systems to isolate splitter functionality
 * from a real file system.
 *
 * @author Janne Valkealahti
 *
 */
public abstract class AbstractSplitterTests {

	protected final static Configuration CONFIGURATION = new Configuration();

	protected static Path mockWithFileSystem(int blockCount, long blockSize) throws Exception {
		return mockWithFileSystem(blockCount, blockSize, 0);
	}

	protected static Path mockWithFileSystem(int blockCount, long blockSize, long extraBlockSize) throws Exception {
		final ArrayList<BlockLocation> blocks = new ArrayList<BlockLocation>();
		long offset = 0;
		int i = 0;
		for (; i < blockCount; i++) {
			blocks.add(new BlockLocation(new String[]{"names"+i}, new String[]{"hosts"+i}, offset, blockSize));
			offset += blockSize;
		}

		// extra just means that we add a non full last block
		if (extraBlockSize > 0 && extraBlockSize < blockSize) {
			blocks.add(new BlockLocation(new String[]{"names"+i}, new String[]{"hosts"+i}, offset, extraBlockSize));
			offset += extraBlockSize;
		}

		FileStatus mStatus = mock(FileStatus.class);
		Path mPath = mock(Path.class);
		FileSystem mFs = mock(FileSystem.class);
		when(mStatus.getLen()).thenReturn(offset);
		when(mStatus.getBlockSize()).thenReturn(blockSize);
		when(mFs.getFileStatus(mPath)).thenReturn(mStatus);

		when(mFs.getFileBlockLocations((FileStatus)any(), anyLong(), anyLong())).thenAnswer(new Answer<BlockLocation[]>() {

			@Override
			public BlockLocation[] answer(InvocationOnMock invocation) throws Throwable {
				 Object[] arguments = invocation.getArguments();
				 return findBlocks(blocks, (Long)arguments[1], (Long)arguments[2]);
			}
		});

		when(mPath.getFileSystem((Configuration)any())).thenReturn(mFs);
		return mPath;
	}

	protected static BlockLocation[] findBlocks(ArrayList<BlockLocation> blocks, long start, long length) {
		final ArrayList<BlockLocation> ret = new ArrayList<BlockLocation>();
		for (BlockLocation block : blocks) {
			if (!((start >= (block.getOffset() + block.getLength())) && ((start + length) <= block.getOffset()))) {
				ret.add(block);
			}
		}
		return ret.toArray(new BlockLocation[0]);
	}

}
