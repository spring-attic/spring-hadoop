/*
 * Copyright 2015 the original author or authors.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.servlet.Filter;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.support.BasicAuthenticationInterceptor;
import org.springframework.stereotype.Controller;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.yarn.YarnSystemConstants;

/**
 * Tests for {@link YarnRestTemplateAutoConfiguration}.
 *
 * @author Janne Valkealahti
 *
 */
public class YarnRestTemplateAutoConfigurationTests {

  private ConfigurableApplicationContext context;

  @After
  public void close() {
    if (context != null) {
      context.close();
    }
    context = null;
  }

  @Test
  public void testDefaultFallback() throws Exception {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.register(FallbackYarnRestTemplateAutoConfiguration.class,
        YarnRestTemplateAutoConfiguration.class);
    context = ctx;
    context.refresh();
    assertThat(context.getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE), notNullValue());
  }

  @Test
  public void testBasicEnabled() throws Exception {
    context = SpringApplication.run(
        new Class<?>[] {YarnRestTemplateAutoConfiguration.class,
            FallbackYarnRestTemplateAutoConfiguration.class, VanillaWebConfiguration.class,
            WebConfiguration.class},
        new String[] {"--security.basic.enabled=true", "--spring.security.user.name=username",
            "--spring.security.user.password=password"});

    PortInitListener portInitListener = context.getBean(PortInitListener.class);
    assertThat(portInitListener.latch.await(10, TimeUnit.SECONDS), is(true));
    int port = portInitListener.port;

    MockMvc mockMvc = MockMvcBuilders.webAppContextSetup((WebApplicationContext) this.context)
        .addFilters(this.context.getBean("springSecurityFilterChain", Filter.class)).build();
    mockMvc.perform(MockMvcRequestBuilders.get("/"))
        .andExpect(MockMvcResultMatchers.status().isUnauthorized()).andExpect(MockMvcResultMatchers
            .header().string("www-authenticate", Matchers.containsString("realm=\"Realm\"")));

    RestTemplate restTemplate =
        context.getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE, RestTemplate.class);

    String response = restTemplate.getForObject("http://localhost:" + port + "/", String.class);
    assertThat(response, is("home"));
  }

  @Test
  public void testBasicEnabledWithSsl() throws Exception {
    context = SpringApplication.run(new Class<?>[] {CustomRestTemplateConfiguration.class,
        YarnRestTemplateAutoConfiguration.class, FallbackYarnRestTemplateAutoConfiguration.class,
        VanillaWebConfiguration.class, WebConfiguration.class},
        new String[] {"--security.basic.enabled=true", "--spring.security.user.name=username",
            "--spring.security.user.password=password", "--security.require-ssl=true",
            "--server.ssl.key-store=classpath:test.jks", "--server.ssl.key-store-password=secret",
            "--server.ssl.key-password=password"});

    PortInitListener portInitListener = context.getBean(PortInitListener.class);
    assertThat(portInitListener.latch.await(10, TimeUnit.SECONDS), is(true));
    int port = portInitListener.port;

    RestTemplate restTemplate =
        context.getBean(YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE, RestTemplate.class);

    String response = restTemplate.getForObject("https://localhost:" + port + "/", String.class);
    assertThat(response, is("home"));
  }

  protected static class PortInitListener
      implements ApplicationListener<ServletWebServerInitializedEvent> {

    public int port;
    public CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
      port = event.getWebServer().getPort();
      latch.countDown();
    }

  }

  @Configuration
  protected static class CustomRestTemplateConfiguration {

    @Bean(name = YarnSystemConstants.DEFAULT_ID_RESTTEMPLATE)
    public RestTemplate restTemplate() throws Exception {
      HttpClientBuilder builder = HttpClientBuilder.create();

      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
          new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build());
      builder.setSSLSocketFactory(sslSocketFactory);
      HttpClient httpClient = builder.build();
      HttpComponentsClientHttpRequestFactory clientHttpRequestFactory =
          new HttpComponentsClientHttpRequestFactory(httpClient);
      RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);
      restTemplate.getInterceptors()
          .add(new BasicAuthenticationInterceptor("username", "password"));
      return restTemplate;

    }

  }

  @Configuration
  protected static class VanillaWebConfiguration {

    @Bean
    public PortInitListener portListener() {
      return new PortInitListener();
    }

    @Bean
    public TomcatServletWebServerFactory tomcatEmbeddedServletContainerFactory() {
      TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
      factory.setPort(0);
      return factory;
    }
  }

  @MinimalWebConfiguration
  @Import({SecurityAutoConfiguration.class, UserDetailsServiceAutoConfiguration.class})
  @Controller
  protected static class WebConfiguration {

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public String home() {
      return "home";
    }

  }

  @Configuration
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @ImportAutoConfiguration({ServletWebServerFactoryAutoConfiguration.class,
      DispatcherServletAutoConfiguration.class, WebMvcAutoConfiguration.class,
      HttpMessageConvertersAutoConfiguration.class, ErrorMvcAutoConfiguration.class,
      PropertyPlaceholderAutoConfiguration.class})
  protected @interface MinimalWebConfiguration {

  }

}
