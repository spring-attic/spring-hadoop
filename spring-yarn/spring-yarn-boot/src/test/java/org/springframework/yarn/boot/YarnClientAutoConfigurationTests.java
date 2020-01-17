/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.yarn.boot;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.yarn.YarnSystemConstants;

/**
 * Tests for {@link YarnClientAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnClientAutoConfigurationTests {

  private AnnotationConfigApplicationContext context;

  @Rule
  public ExpectedException expected = ExpectedException.none();

  @After
  public void close() {
    if (context != null) {
      context.close();
    }
    System.clearProperty("HADOOP_TOKEN_FILE_LOCATION");
  }

  @Test
  public void testOutside() throws Exception {
    context = new AnnotationConfigApplicationContext();
    context.register(YarnClientAutoConfiguration.class);
    context.refresh();
    assertThat(context.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT), is(true));
  }

  @Test
  public void testOnContainer() throws Exception {
    System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00002");

    context = new AnnotationConfigApplicationContext();
    context.register(YarnClientAutoConfiguration.class);
    context.refresh();
    assertThat(context.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT), is(false));
  }

  @Test
  public void testOnContainerForceOverride() throws Exception {
    System.setProperty("HADOOP_TOKEN_FILE_LOCATION", "xx/xx/xx/00002");

    context = new AnnotationConfigApplicationContext();

    TestPropertyValues.of("spring.yarn.client.enabled=true").applyTo(context);

    context.register(YarnClientAutoConfiguration.class);
    context.refresh();
    assertThat(context.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT), is(true));
  }

  @Test
  public void testOutsideForceOverride() throws Exception {
    context = new AnnotationConfigApplicationContext();

    TestPropertyValues.of("spring.yarn.client.enabled=false").applyTo(context);

    context.register(YarnClientAutoConfiguration.class);
    context.refresh();
    assertThat(context.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT), is(false));
  }
}
