/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.boot.actuate.endpoint.mvc;

import org.springframework.util.StringUtils;

public class ContainerClusterModifyRequest extends AbstractContainerClusterRequest {

	private String action;

	public void setAction(String action) {
		this.action = action;
	}

	public String getAction() {
		return action;
	}

	public static ModifyAction getModifyAction(String action) {
		if (StringUtils.hasText(action)) {
			return ModifyAction.valueOf(action.toUpperCase());
		}
		return null;
	}

	public enum ModifyAction {
		START,
		STOP
	}

}
