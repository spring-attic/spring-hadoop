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
package org.springframework.yarn.batch.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration("master-ns.xml")
public class MasterNamespaceTest {

	@Autowired
	private ApplicationContext ctx;

//    @Test
//    public void testSomething() {
//        assertTrue(ctx.containsBean("yarnAppmaster"));
//        BatchAppmasterFactoryBean fb = (BatchAppmasterFactoryBean) ctx.getBean("&yarnAppmaster");
//        assertNotNull(fb);
//    }

}
