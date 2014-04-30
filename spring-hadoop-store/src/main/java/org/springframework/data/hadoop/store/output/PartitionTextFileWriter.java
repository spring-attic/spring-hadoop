package org.springframework.data.hadoop.store.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.NotSupportedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.PartitionDataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
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

	//holds a TextFileWriter instance for each partition
	protected ConcurrentMap<String, TextFileWriter> partitionWriters = new ConcurrentHashMap<String, TextFileWriter>();
	
	//TextFileWriter stuff
	private final byte[] delimiter;

	private Configuration configuration;
	private Path basePath;
	private CodecInfo codec;
	private FileNamingStrategy strategy;
	protected TextFileWriter nextWriter; 
	protected FileSystem fs;
	@Override
	 public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		this.strategy = fileNamingStrategy;
	}
	protected TextFileWriter createWriterForDirectory (String directory) {
		//TODO: here, we're creating the TextFileWriter instead of having the spring context to create it for us. 
		//we can alwasy do getBean getBeanFactory().getBean to have the spring context to create it. Pros and cons ?
		
		TextFileWriter writer = new TextFileWriter(configuration, Path.mergePaths(basePath, new Path(directory)), codec, delimiter);
		writer.setFileNamingStrategy(this.strategy);
		this.createDirectoryIfNotExist(directory);
		
		return writer;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		
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
	
	protected synchronized void createDirectoryIfNotExist(String directory) {
		Path p = new Path(directory);
		try {
		if (fs.exists(p) == false)
			 fs.mkdirs(p); //according to the API this will create all the directory structure! 
		}
		catch (Exception e) {}
			
	}

	@Override
	public void writeToPartition(String directory, String message) throws IOException {
		TextFileWriter writer = partitionWriters.putIfAbsent(directory, createWriterForDirectory(directory));
		writer.write(message);
	}

}
