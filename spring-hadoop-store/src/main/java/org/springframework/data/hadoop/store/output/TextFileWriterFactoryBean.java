package org.springframework.data.hadoop.store.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.support.StoreUtils;

public class TextFileWriterFactoryBean implements FactoryBean<TextFileWriter> {

	@Override
	public TextFileWriter getObject() throws Exception {
		TextFileWriter writer = new TextFileWriter( configuration,  basePath,  codec,  delimiter) ;
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setRolloverStrategy(rollOverStrategy);
		return writer;
	}

	@Override
	public Class<?> getObjectType() {
		return TextFileWriter.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}
	public FileNamingStrategy getFileNamingStrategy() {
		return fileNamingStrategy;
	}

	public void setFileNamingStrategy(FileNamingStrategy fileNamingStrategy) {
		this.fileNamingStrategy = fileNamingStrategy;
	}

	public RolloverStrategy getRollOverStrategy() {
		return rollOverStrategy;
	}

	public void setRollOverStrategy(RolloverStrategy rollOverStrategy) {
		this.rollOverStrategy = rollOverStrategy;
	}
	private FileNamingStrategy fileNamingStrategy;
	private RolloverStrategy rollOverStrategy;
	
	private Configuration configuration;
	private Path basePath;
	private CodecInfo codec;
	private byte[] delimiter = StoreUtils.getUTF8DefaultDelimiter();
	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Path getBasePath() {
		return basePath;
	}

	public void setBasePath(Path basePath) {
		this.basePath = basePath;
	}

	public CodecInfo getCodec() {
		return codec;
	}

	public void setCodec(CodecInfo codec) {
		this.codec = codec;
	}

	public byte[] getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(byte[] delimiter) {
		this.delimiter = delimiter;
	}
	

}
