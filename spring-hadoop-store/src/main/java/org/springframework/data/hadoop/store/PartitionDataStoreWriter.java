package org.springframework.data.hadoop.store;

import java.io.IOException;

public interface PartitionDataStoreWriter<T> extends DataStoreWriter<T> {
	void writeToPartition(String directory, T message) throws IOException;
}
