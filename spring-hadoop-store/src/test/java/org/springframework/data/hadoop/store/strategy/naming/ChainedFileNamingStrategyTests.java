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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link ChainedFileNamingStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class ChainedFileNamingStrategyTests {

	@Test
	public void testResolveNullPath() {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid", true));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy(".extension"));

		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0.extension"));
	}

	@Test
	public void testOrdering() {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		StaticFileNamingStrategy strategy1 = new StaticFileNamingStrategy("base");
		strategy1.setOrder(1);
		strategies.add(strategy1);
		UuidFileNamingStrategy strategy2 = new UuidFileNamingStrategy("fakeuuid", true);
		strategy2.setOrder(2);
		strategies.add(strategy2);
		RollingFileNamingStrategy strategy3 = new RollingFileNamingStrategy();
		strategy3.setOrder(3);
		strategies.add(strategy3);
		StaticFileNamingStrategy strategy4 = new StaticFileNamingStrategy(".extension");
		strategy4.setOrder(4);
		strategies.add(strategy4);

		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0.extension"));

		strategy = new ChainedFileNamingStrategy();
		strategy.register(strategy4);
		strategy.register(strategy3);
		strategy.register(strategy2);
		strategy.register(strategy1);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0.extension"));

		strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(strategy4);
		strategies.add(strategy3);
		strategies.add(strategy2);
		strategies.add(strategy1);
		strategy = new ChainedFileNamingStrategy(strategies);
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0.extension"));

		strategy = strategy.createInstance();
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-0.extension"));
	}

	@Test
	public void testFoo() {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid", true));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy("extension", "."));

		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);
		assertThat(strategy.init(new Path("/foo/base-fakeuuid-0.extension")), nullValue());
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-1.extension"));
		strategy.next();
		assertThat(strategy.resolve(null).toString(), is("base-fakeuuid-2.extension"));
	}

	@Test
	public void testFoo2() {
		List<FileNamingStrategy> strategies = new ArrayList<FileNamingStrategy>();
		strategies.add(new StaticFileNamingStrategy("base"));
		strategies.add(new UuidFileNamingStrategy("fakeuuid", false));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy("extension", "."));

		ChainedFileNamingStrategy strategy = new ChainedFileNamingStrategy(strategies);
		assertThat(strategy.init(new Path("/foo/base-0.extension")), nullValue());
		assertThat(strategy.resolve(null).toString(), is("base-1.extension"));
		strategy.next();
		assertThat(strategy.resolve(null).toString(), is("base-2.extension"));
	}

}
