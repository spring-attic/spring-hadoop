/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.hadoop.configuration;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.hadoop.util.VersionUtils;
import org.springframework.util.ReflectionUtils;

import static org.junit.Assert.*;

/**
 * See SHDP-92: https://jira.springsource.org/browse/SHDP-92
 * @author Costin Leau
 */
public class UrlInfiniteLoopTest {

    @Before
    public void setUp() {
        if (VersionUtils.isHadoop2X()) {
            // Avoid potential StackOverflowError for Hadoop 2.0.x (see SHDP-111)
            if (VersionUtils.isYarnAvailable()) {
                try {
                    Configuration conf = (Configuration) Class.forName("org.apache.hadoop.yarn.conf.YarnConfiguration").newInstance();
                    Method getFileSystemClass =
                            ReflectionUtils.findMethod(FileSystem.class, "getFileSystemClass",
                                    String.class, Configuration.class);
                    getFileSystemClass.invoke(null, "hdfs", conf);
                } catch (Exception e) {}
            } else {
                try {
                    Configuration conf = (Configuration) Class.forName("org.apache.hadoop.conf.Configuration").newInstance();
                    Method getFileSystemClass =
                            ReflectionUtils.findMethod(FileSystem.class, "getFileSystemClass",
                                    String.class, Configuration.class);
                    getFileSystemClass.invoke(null, "hdfs", conf);
                } catch (Exception e) {}
            }
        }
    }

	@Test
	public void testInfiniteLoop() throws Exception {
		ConfigurationFactoryBean factory = new ConfigurationFactoryBean();
		factory.setRegisterUrlHandler(true);
		factory.afterPropertiesSet();
		assertNotNull(factory.getObject());
		FileSystem fs = FileSystem.get(factory.getObject());
		assertNotNull(fs);
	}

}
