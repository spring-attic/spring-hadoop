package org.springframework.data.hadoop.store.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.NotSupportedException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.springframework.data.hadoop.store.PartitionDataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.support.StoreUtils;

/**
 * Represents a Store Writer that can write to several directories. 
 * It delegate all the writing logic to TextFileWriter instances, one for each directory
 * which are held using a ConcurrentMap
 * since 1.0.0.M7 ???
 * @author Rodrigo Meneses
 *
 */

///TODO:
public class PartitionTextFileWriter extends AbstractDataStreamWriter implements PartitionDataStoreWriter<String>{
	private final static Log log = LogFactory.getLog(PartitionTextFileWriter.class);

	//holds a TextFileWriter instance for each partition
	protected ConcurrentMap<String, TextFileWriter> partitionWriters = new ConcurrentHashMap<String, TextFileWriter>();
	
	//TextFileWriter stuff
	private final byte[] delimiter;

	private Configuration configuration;
	private Path basePath;
	private CodecInfo codec;
	private FileNamingStrategy strategy;
	private RolloverStrategy rollOver;
	private String strategyBeanName;
	private String rollOverBeanName;
	public String getStrategyBeanName() {
		return strategyBeanName;
	}

	public void setStrategyBeanName(String strategyBeanName) {
		this.strategyBeanName = strategyBeanName;
	}

	public String getRollOverBeanName() {
		return rollOverBeanName;
	}

	public void setRollOverBeanName(String rollOverBeanName) {
		this.rollOverBeanName = rollOverBeanName;
	}

	protected TextFileWriter nextWriter; 
	protected FileSystem fs;
	protected String internalWriterName;
	protected long idleTimeout;
	@Override
	public void setRolloverStrategy(RolloverStrategy rolloverStrategy) {
		this.rollOver = rolloverStrategy;
	}
	
	@Override
	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}
	
	@Override
	 public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		this.strategy = fileNamingStrategy;
	}
	protected TextFileWriter createWriterForDirectory (String directory) {
		//TODO: here, we're creating the TextFileWriter instead of having the spring context to create it for us. 
		//we can alwasy do getBean getBeanFactory().getBean to have the spring context to create it. Pros and cons ?
		Path finalDir = Path.mergePaths(basePath, new Path(directory));	
		TextFileWriter writer = (TextFileWriter) getBeanFactory().getBean(internalWriterName, configuration, finalDir, codec, delimiter, idleTimeout);
		this.strategy = getBeanFactory().getBean(strategyBeanName, FileNamingStrategy.class);
		this.rollOver = getBeanFactory().getBean(rollOverBeanName, RolloverStrategy.class);
		writer.setFileNamingStrategy(this.strategy);
		writer.setRolloverStrategy(this.rollOver);
		writer.start();
		this.createDirectoryIfNotExist(finalDir);
		return writer;
	}
	

	public String getInternalWriterName() {
		return internalWriterName;
	}

	public void setInternalWriterName(String internalWriterName) {
		this.internalWriterName = internalWriterName;
	}

	/**
	 * Instantiates a new text file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 */
	public PartitionTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
		this(configuration, basePath, codec, StoreUtils.getUTF8DefaultDelimiter());
	}

	/**
	 * Instantiates a new text file writer.
	 *
	 * @param configuration the hadoop configuration
	 * @param basePath the hdfs path
	 * @param codec the compression codec info
	 * @param delimiter the delimiter
	 */
	public PartitionTextFileWriter(Configuration configuration, Path basePath, CodecInfo codec, byte[] delimiter) {
		super(configuration, basePath,codec);
		this.configuration=configuration;
		this.basePath=basePath;
		this.codec=codec;
		this.delimiter=delimiter;
		
		try {
			this.fs = FileSystem.get(configuration);
		} catch (IOException e) {
			log.error("Error creating FS hadoop instance in partition writer", e);
		}
	}
	
	
	@Override
	public void write(String entity) throws IOException {
		//TODO: we shoud
	}

	@Override
	public void flush() throws IOException {
		for ( TextFileWriter w : partitionWriters.values()) {
			try {
				w.flush();
			}
			catch (Exception e) {
				
			}
		}
	}

	@Override
	public void close() throws IOException {
		
		for ( TextFileWriter w : partitionWriters.values()) {
			try {
				w.close();
			}
			catch (Exception e) {
				
			}
		}
	}
	
	protected synchronized void createDirectoryIfNotExist(Path directory) {

		try {
		if (fs.exists(directory) == false)
			 fs.mkdirs(directory); //according to the API this will create all the directory structure! 
		}
		catch (Exception e) {
			log.error("Failure while creating directory " + directory.toString(), e);
		}
			
	}

	@Override
	public void writeToPartition(String directory, String message) throws IOException {
		TextFileWriter writer = null;
		if (partitionWriters.containsKey(directory)==false) {
			writer = createWriterForDirectory(directory);
			partitionWriters.put(directory,  writer);
		}
		else {
			writer = partitionWriters.get(directory);
		}
		writer.write(message);
		 
	}

}
