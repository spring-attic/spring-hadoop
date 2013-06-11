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
package org.springframework.yarn.integration.ip.mind.binding;

/**
 * Base object for messages which should have a common
 * fields for responses. Fields can be used to tell
 * state and error of a response.
 *
 * @author Janne Valkealahti
 *
 */
public class BaseResponseObject extends BaseObject {

	/** Message of a response. */
	public String resmsg;

	/** State of a response. */
	public String resstate;

	public BaseResponseObject() {
	}

	public BaseResponseObject(String type) {
		super(type);
	}

	public BaseResponseObject(String type, String resmsg, String resstate) {
		super(type);
		this.resmsg = resmsg;
		this.resstate = resstate;
	}

	public String getResmsg() {
		return resmsg;
	}

	public void setResmsg(String resmsg) {
		this.resmsg = resmsg;
	}

	public String getResstate() {
		return resstate;
	}

	public void setResstate(String resstate) {
		this.resstate = resstate;
	}

}
