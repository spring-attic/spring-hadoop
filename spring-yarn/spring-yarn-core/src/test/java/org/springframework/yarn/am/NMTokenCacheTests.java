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
package org.springframework.yarn.am;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.junit.Test;
import org.springframework.yarn.support.compat.NMTokenCacheCompat;

/**
 * Tests for Hadoops {@code NMTokenCache}. At a time writing this test hadoop
 * vanilla 2.2.0 GA is using static methods in this class, however cloudera
 * with 2.2.0-cdh5.0.0-beta-1 correctly modified this class with singleton
 * pattern and removed other static methods. This class makes sure
 * we work around this correctly.
 *
 * @author Janne Valkealahti
 *
 */
public class NMTokenCacheTests {

	@SuppressWarnings("static-access")
	@Test
	public void testTokenCacheGenericUsage() {
		// cdh
		// static org.apache.hadoop.yarn.client.api.NMTokenCache.getSingleton()
		// void org.apache.hadoop.yarn.client.api.NMTokenCache.setNMToken(String, Token)

		// vanilla
		// static void org.apache.hadoop.yarn.client.api.NMTokenCache.setNMToken(String, Token)

		NMTokenCache cache1 = NMTokenCacheCompat.getNMTokenCache();
		NMTokenCache cache2 = NMTokenCacheCompat.getNMTokenCache();
		cache1.setNMToken("nodeAddr1", new TestToken("kind1"));
		cache2.setNMToken("nodeAddr2", new TestToken("kind2"));

		assertThat(cache1.getNMToken("nodeAddr1").getKind(), is("kind1"));
		assertThat(cache2.getNMToken("nodeAddr1").getKind(), is("kind1"));
		assertThat(cache1.getNMToken("nodeAddr2").getKind(), is("kind2"));
		assertThat(cache2.getNMToken("nodeAddr2").getKind(), is("kind2"));

		assertThat(cache1, sameInstance(cache2));
		assertThat(NMTokenCacheCompat.getNMTokenCache(), sameInstance(NMTokenCacheCompat.getNMTokenCache()));
	}

	private static class TestToken extends Token {

		String kind;

		public TestToken(String kind) {
			this.kind = kind;
		}

		@Override
		public ByteBuffer getIdentifier() {
			return null;
		}

		@Override
		public void setIdentifier(ByteBuffer identifier) {
		}

		@Override
		public ByteBuffer getPassword() {
			return null;
		}

		@Override
		public void setPassword(ByteBuffer password) {
		}

		@Override
		public String getKind() {
			return kind;
		}

		@Override
		public void setKind(String kind) {
			this.kind = kind;
		}

		@Override
		public String getService() {
			return null;
		}

		@Override
		public void setService(String service) {
		}

	}

}
