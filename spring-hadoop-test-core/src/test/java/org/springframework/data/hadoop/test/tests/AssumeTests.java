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
package org.springframework.data.hadoop.test.tests;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;

/**
 * Tests for {@link Assume}.
 *
 * @author Janne Valkealahti
 */
public class AssumeTests {

	@Test
	public void testCodecNotExist() {
		AssumptionViolatedException ave = null;
		try {
			Assume.codecExists("foo.Jee");
		} catch (AssumptionViolatedException e) {
			ave = e;
		}
		assertThat(ave, notNullValue());

		ave = null;
		try {
			Assume.codecExists("org.apache.hadoop.conf.Configuration");
		} catch (AssumptionViolatedException e) {
			ave = e;
		}
		assertThat(ave, notNullValue());

		ave = null;
		try {
			Assume.codecExists("org.apache.hadoop.io.compress.GzipCodec");
		} catch (AssumptionViolatedException e) {
			ave = e;
		}
		assertThat(ave, nullValue());
	}


}
