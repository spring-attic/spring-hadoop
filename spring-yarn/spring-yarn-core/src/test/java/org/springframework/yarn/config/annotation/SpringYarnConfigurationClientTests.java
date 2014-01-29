package org.springframework.yarn.config.annotation;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigurer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
public class SpringYarnConfigurationClientTests {

	@Autowired
	private ApplicationContext ctx;

	@Test
	public void testSimpleConfig() throws Exception {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean(YarnSystemConstants.DEFAULT_ID_CLIENT));
		YarnClient client = ctx.getBean(YarnSystemConstants.DEFAULT_ID_CLIENT, YarnClient.class);
		assertNotNull(client);
	}

	@Configuration
	@EnableYarn(enable=Enable.CLIENT)
	static class Config extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnClientConfigurer client) throws Exception {
			client
				.withMasterRunner();
		}

	}

}
