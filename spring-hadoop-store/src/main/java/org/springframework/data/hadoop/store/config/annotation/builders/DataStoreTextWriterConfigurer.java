/*
 * Copyright 2014-2015 the original author or authors.
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
package org.springframework.data.hadoop.store.config.annotation.builders;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.codec.Codecs;
import org.springframework.data.hadoop.store.config.annotation.SpringDataStoreTextWriterConfigurerAdapter;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultNamingStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultPartitionStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.DefaultRolloverStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.NamingStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.PartitionStrategyConfigurer;
import org.springframework.data.hadoop.store.config.annotation.configurers.RolloverStrategyConfigurer;

/**
 * {@code DataStoreTextWriterConfigurer} is an interface for {@code DataStoreTextWriterBuilder} which is
 * exposed to user via {@link SpringDataStoreTextWriterConfigurerAdapter}.
 * <p>
 * Typically configuration is shown below.
 * <br>
 * <pre>
 * &#064;Configuration
 * &#064;EnableDataStoreTextWriter
 * static class Config extends SpringDataStoreTextWriterConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
 *     writer
 *       .basePath("/tmp/foo1");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface DataStoreTextWriterConfigurer {

	/**
	 * Specify a writer's partition strategy. Applies a new {@link DefaultPartitionStrategyConfigurer}
	 * into current builder.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withPartitionStrategy()
	 *       .map("spelexpression");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link PartitionStrategyConfigurer} for chaining
	 * @throws Exception exception
	 */
	PartitionStrategyConfigurer withPartitionStrategy() throws Exception;

	/**
	 * Specify a writer's naming strategy. Applies a new {@link DefaultNamingStrategyConfigurer}
	 * into current builder.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withNamingStrategy()
	 *       .name("foo")
	 *       .uuid()
	 *       .rolling()
	 *       .codec();
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link NamingStrategyConfigurer} for chaining
	 * @throws Exception exception
	 */
	NamingStrategyConfigurer withNamingStrategy() throws Exception;

	/**
	 * Specify a writer's rollover strategy. Applies a new {@link DefaultRolloverStrategyConfigurer}
	 * into current builder.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .withRolloverStrategy()
	 *       .size("1M");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @return {@link RolloverStrategyConfigurer} for chaining
	 * @throws Exception exception
	 */
	RolloverStrategyConfigurer withRolloverStrategy() throws Exception;

	/**
	 * Specify a writer's Hadoop configuration.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .configuration(new Configuration());
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param configuration the hadoop configuration
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer configuration(Configuration configuration);

	/**
	 * Specify a writer's base path.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .basePath(new Path("/my/path"));
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param path the path
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer basePath(Path path);

	/**
	 * Specify a writer's base path.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .basePath("/my/path");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param path the path
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer basePath(String path);

	/**
	 * Specify a writer's compression coded.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .codec(Codecs.BZIP2.getCodecInfo());
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param codec the codec
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer codec(CodecInfo codec);

	/**
	 * Specify a writer's compression coded.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .codec("gzip");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param codec the codec
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer codec(String codec);

	/**
	 * Specify a writer's compression coded.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .codec(Codecs.BZIP2);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param codec the codec
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer codec(Codecs codec);

	/**
	 * Specify if writer is allowed to overwrite files.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .overwrite(false);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param overwrite enable overwrite
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer overwrite(boolean overwrite);

	/**
	 * Specify if writer is allowed to do file append.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .append(false);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param append enable append support
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer append(boolean append);

	/**
	 * Specify a writer's in-use prefix.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .inWritingPrefix("myprefix");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param prefix the in-writing prefix
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer inWritingPrefix(String prefix);

	/**
	 * Specify a writer's in-use suffix.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .inWritingSuffix("mysuffix");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param suffix the in-writing suffix
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer inWritingSuffix(String suffix);

	/**
	 * Specify a writer's idle timeout.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .idleTimeout(60000);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param timeout the idle timeout
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer idleTimeout(long timeout);

	/**
	 * Specify a writer's close timeout.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .closeTimeout(60000);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param timeout the close timeout
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer closeTimeout(long timeout);

	/**
	 * Specify a writer's max file open attempts.
	 *
	 * <br>
	 * <br>JavaConfig:
	 * <br>
	 * <pre>
	 *
	 * public void configure(DataStoreTextWriterConfigurer writer) throws Exception {
	 *   writer
	 *     .fileOpenAttempts(10);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <br>
	 * No equivalent
	 *
	 * @param attempts the attemps count
	 *
	 * @return {@link DataStoreTextWriterConfigurer} for chaining
	 */
	DataStoreTextWriterConfigurer fileOpenAttempts(int attempts);

}
