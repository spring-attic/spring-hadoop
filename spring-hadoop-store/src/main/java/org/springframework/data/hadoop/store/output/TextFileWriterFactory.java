package org.springframework.data.hadoop.store.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.support.LifecycleObjectSupport;

/**
 * A factory to create TextFileWriters for use by a {@link ShardedDataStoreWriter}
 * @author Duncan McIntyre
 *
 */
public class TextFileWriterFactory implements DataStoreWriterFactory<DataStoreWriter<String>> {
	
	@Override
	public TextFileWriter createWriter(
			final Configuration configuration,
			final Path basePath, CodecInfo codec, 
			final ShardedDataStoreWriter<?> parent) {
		
		
		TextFileWriter writer = new TextFileWriter(configuration, basePath, codec) {
			@Override
			public synchronized void close() throws IOException {
				// catch close() and destroy from parent
				// this needs to happen before we pass
				// close() to writer
				parent.destroyWriter(basePath);
				super.close();
			}
		};
		
		if (parent.getBeanFactory() != null) {
			writer.setBeanFactory(parent.getBeanFactory());
		}
		writer.setPhase(parent.getPhase());
		if (parent.getTaskExecutor() != null) {
			writer.setTaskExecutor(parent.getTaskExecutor());
		}
		if (parent.getTaskScheduler() != null) {
			writer.setTaskScheduler(parent.getTaskScheduler());
		}
		writer.setAutoStartup(parent.isAutoStartup());
		if (parent.getStoreEventPublisher() != null) {
			writer.setStoreEventPublisher(parent.getStoreEventPublisher());
		}
		if (parent.getFileNamingStrategyFactory() != null) {
			writer.setFileNamingStrategy(parent.getFileNamingStrategyFactory().createInstance());
		}
		if (parent.getRolloverStrategyFactory() != null) {
			writer.setRolloverStrategy(parent.getRolloverStrategyFactory().createInstance());
		}
		writer.setIdleTimeout(parent.getIdleTimeout());
		writer.setOverwrite(parent.isOverwrite());
		writer.setAppendable(parent.isAppendable());
		writer.setInWritingPrefix(parent.getInWritingPrefix());
		writer.setInWritingSuffix(parent.getInWritingSuffix());
		writer.setMaxOpenAttempts(parent.getMaxOpenAttempts());
		writer.afterPropertiesSet();
		writer.start();
		return writer;
	}
}
