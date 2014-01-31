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
package org.springframework.yarn.config.annotation.configurers;

import java.util.Map;

import org.springframework.data.hadoop.config.common.annotation.AnnotationConfigurerBuilder;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigurer;
import org.springframework.yarn.config.annotation.configurers.DefaultLocalResourcesCopyConfigurer.ConfiguredCopyEntry;

public interface LocalResourcesCopyConfigurer extends AnnotationConfigurerBuilder<YarnResourceLocalizerConfigurer> {

	LocalResourcesCopyConfigurer copy(String src, String dest, boolean staging);

	LocalResourcesCopyConfigurer copy(String[] srcs, String dest, boolean staging);

	ConfiguredCopyEntry source(String source);

	LocalResourcesCopyConfigurer raw(String fileName, byte[] content, String dest);

	LocalResourcesCopyConfigurer raw(Map<String, byte[]> raw, String dest);

}
