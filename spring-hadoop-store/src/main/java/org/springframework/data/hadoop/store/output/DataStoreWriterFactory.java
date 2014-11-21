package org.springframework.data.hadoop.store.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.LifecycleObjectSupport;

public interface DataStoreWriterFactory<T extends DataStoreWriter<?>> {
	
	/**
	 *
	 * @param configuration the configuration
	 * @param basePath the base path
	 * @param codec the codec
	 * @return the data store writer
	 */
	T createWriter(Configuration configuration, Path basePath, CodecInfo codec, ShardedDataStoreWriter<?> parent);
}
