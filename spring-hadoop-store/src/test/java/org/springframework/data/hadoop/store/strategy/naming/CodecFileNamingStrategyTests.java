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
import org.springframework.data.hadoop.store.codec.Codecs;

/**
 * Tests for {@link CodecFileNamingStrategy}.
 *
 * @author Janne Valkealahti
 *
 */
public class CodecFileNamingStrategyTests {

	@Test
	public void testResolveNullPath() {
		CodecFileNamingStrategy strategy = new CodecFileNamingStrategy();
		assertThat(strategy.resolve(null), nullValue());
		strategy.setCodecInfo(Codecs.GZIP.getCodecInfo());
		assertThat(strategy.resolve(null).toString(), is(".gz"));
	}

	@Test
	public void testEnabled() {
		CodecFileNamingStrategy strategy = new CodecFileNamingStrategy();
		strategy.setCodecInfo(Codecs.GZIP.getCodecInfo());
		assertThat(strategy.isEnabled(), is(true));
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee.gz"));
		strategy.setEnabled(false);
		assertThat(strategy.isEnabled(), is(false));
		assertThat(strategy.resolve(new Path("/foo/jee")).toString(), is("/foo/jee"));
	}

}
