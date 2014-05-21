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
package org.springframework.data.hadoop.store.strategy.naming;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link UuidFileNamingStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class UuidFileNamingStrategyTests {

	@Test
	public void testResolveNullPath() {
		UuidFileNamingStrategy strategy = new UuidFileNamingStrategy();
		strategy.setEnabled(true);
		assertThat(strategy.resolve(null).toString().length(), is(37));
	}

	@Test
	public void testInit() {
		UuidFileNamingStrategy strategy = new UuidFileNamingStrategy("XXX-123-YYY");
		assertThat(strategy.init(new Path("/foo/jee-XXX-123-YYY")).toString(), is("/foo/jee"));
	}

	@Test
	public void testNewInstance() {
		UuidFileNamingStrategy strategy1 = new UuidFileNamingStrategy();
		strategy1.setOrder(123);
		strategy1.setEnabled(true);
		strategy1.setUuid("foo");
		UuidFileNamingStrategy strategy2 = strategy1.createInstance();
		assertThat(strategy2.getOrder(), is(strategy1.getOrder()));
		assertThat(strategy2.isEnabled(), is(strategy1.isEnabled()));
		assertThat(strategy2.getUuid(), is(strategy1.getUuid()));
	}

	@Test
	public void testResolveWithGivenPath() {
		UuidFileNamingStrategy strategy = new UuidFileNamingStrategy("fakeuuid");
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-fakeuuid"));
		strategy.next();
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee-fakeuuid"));
	}

	@Test
	public void testEnabled() {
		UuidFileNamingStrategy strategy = new UuidFileNamingStrategy("fakeuuid");
		assertThat(strategy.isEnabled(), is(true));
		assertThat(strategy.resolve(null).toString(), is("-fakeuuid"));
		strategy.setEnabled(false);
		assertThat(strategy.isEnabled(), is(false));
		assertThat(strategy.resolve(null), nullValue());
	}

}
