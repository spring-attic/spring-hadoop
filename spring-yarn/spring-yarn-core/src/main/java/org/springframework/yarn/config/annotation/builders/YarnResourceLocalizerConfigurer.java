/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.yarn.config.annotation.builders;

import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.configurers.DefaultLocalResourcesHdfsConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesCopyConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultLocalResourcesCopyConfigurer;
import org.springframework.yarn.config.annotation.configurers.LocalResourcesHdfsConfigurer;

/**
 * Interface for {@link YarnResourceLocalizerBuilder} used from
 * a {@link SpringYarnConfigurerAdapter}.
 * <p>
 * Typically configuration is used as shown below.
 * <br>
 * <pre>
 * &#064;Configuration
 * &#064;EnableYarn
 * static class Config extends SpringYarnConfigurerAdapter {
 *
 *   &#064;Override
 *   public void configure(YarnResourceLocalizerConfigure localizer) throws Exception {
 *     localizer
 *       .withCopy()
 *         .copy("foo.jar", "/tmp", true)
 *         .and()
 *       .withHdfs()
 *         .hdfs("/tmp/foo.jar");
 *   }
 *
 * }
 * </pre>
 *
 * @author Janne Valkealahti
 *
 */
public interface YarnResourceLocalizerConfigurer {

	/**
	 * Specify configuration options as properties with a {@link DefaultLocalResourcesCopyConfigurer}.
	 *
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnResourceLocalizerConfigure localizer) throws Exception {
	 *   localizer
	 *     .withCopy()
	 *       .copy("foo.jar", "/tmp", true);
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:localresources&gt;
	 *   &lt;:hdfs path="/tmp/foo.jar" staging="false"/&gt;
	 * &lt;/yarn:localresources&gt;
	 * </pre>
	 *
	 * @return {@link LocalResourcesCopyConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	LocalResourcesCopyConfigurer withCopy() throws Exception;

	/**
	 * Specify configuration options as properties with a {@link DefaultLocalResourcesHdfsConfigurer}.
	 *
	 * <br>JavaConfig:
	 * <pre>
	 * public void configure(YarnResourceLocalizerConfigure localizer) throws Exception {
	 *   localizer
	 *     .withHdfs()
	 *       .hdfs("/tmp/foo.jar");
	 * }
	 * </pre>
	 *
	 * <br>XML:
	 * <pre>
	 * &lt;yarn:localresources&gt;
	 *   &lt;:hdfs path="/tmp/foo.jar" staging="false"/&gt;
	 * &lt;/yarn:localresources&gt;
	 * </pre>
	 *
	 * @return {@link LocalResourcesHdfsConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	LocalResourcesHdfsConfigurer withHdfs() throws Exception;

	/**
	 * Specify configuration options as properties with a {@link DefaultLocalResourcesHdfsConfigurer}
	 * with an identifier.
	 *
	 * @param id the identifier
	 * @return {@link LocalResourcesHdfsConfigurer} for chaining
	 * @throws Exception if error occurred
	 * @see #withHdfs()
	 */
	LocalResourcesHdfsConfigurer withHdfs(String id) throws Exception;

	/**
	 * Specify a staging directory.
	 *
	 * @param stagingDirectory the staging directory
	 * @return {@link YarnResourceLocalizerConfigurer} for chaining
	 * @throws Exception if error occurred
	 */
	YarnResourceLocalizerConfigurer stagingDirectory(String stagingDirectory) throws Exception;

}
