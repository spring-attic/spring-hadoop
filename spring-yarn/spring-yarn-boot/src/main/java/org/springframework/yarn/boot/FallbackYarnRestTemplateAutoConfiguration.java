/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.boot;

import org.apache.http.client.HttpClient;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.YarnSystemConstants;

@Configuration
@ConditionalOnMissingBean(name = YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE)
@ConditionalOnClass({ HttpClient.class, RestTemplate.class })
@AutoConfigureAfter(YarnRestTemplateAutoConfiguration.class)
public class FallbackYarnRestTemplateAutoConfiguration {

	@Bean(name = YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE)
	public RestTemplate restTemplate() {
		return new RestTemplate(new HttpComponentsClientHttpRequestFactory());
	}

}
