/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.config.common.annotation.importing;

import java.util.UUID;

import org.springframework.data.hadoop.config.common.annotation.AbstractConfiguredAnnotationBuilder;

public class ImportingTestConfigBuilder
		extends
		AbstractConfiguredAnnotationBuilder<ImportingTestConfig, ImportingTestConfigBuilder, ImportingTestConfigBuilder> {

	private UUID uuid;

	@Override
	protected ImportingTestConfig performBuild() throws Exception {
		ImportingTestConfig bean = new ImportingTestConfig();
		bean.setImportingData(uuid);
		return bean;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

}
