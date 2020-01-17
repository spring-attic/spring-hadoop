/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.yarn.support;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.text.ParseException;

import org.junit.Test;

/**
 * Tests for {@code ParsingUtils}.
 *
 * @author Janne Valkealahti
 *
 */
public class ParsingUtilsTests {

	@Test
	public void testParseBytesAsMegs() throws ParseException {
		assertThat(ParsingUtils.parseBytesAsMegs("0"), is(0L));
		assertThat(ParsingUtils.parseBytesAsMegs("1"), is(1L));
		assertThat(ParsingUtils.parseBytesAsMegs("1024K"), is(1L));
		assertThat(ParsingUtils.parseBytesAsMegs("1124K"), is(1L));
		assertThat(ParsingUtils.parseBytesAsMegs("2047K"), is(1L));
		assertThat(ParsingUtils.parseBytesAsMegs("2048K"), is(2L));
		assertThat(ParsingUtils.parseBytesAsMegs("2048k"), is(2L));
		assertThat(ParsingUtils.parseBytesAsMegs("1M"), is(1L));
		assertThat(ParsingUtils.parseBytesAsMegs("2M"), is(2L));
		assertThat(ParsingUtils.parseBytesAsMegs("2m"), is(2L));
		assertThat(ParsingUtils.parseBytesAsMegs("1G"), is(1024L));
		assertThat(ParsingUtils.parseBytesAsMegs("1g"), is(1024L));
	}

}
