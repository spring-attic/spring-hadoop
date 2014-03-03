package org.springframework.yarn.test.context;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
@DirtiesContext(classMode=ClassMode.AFTER_EACH_TEST_METHOD)
public class DirtiesContextTests {

	@Autowired
	private ApplicationContext ctx;
	
	@Resource(name = "yarnConfiguration")
	Configuration configuration;
	
	@Test
	public void testCluster1() {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean("yarnCluster"));
		assertTrue(ctx.containsBean("yarnConfiguration"));
		Configuration config = (Configuration) ctx.getBean("yarnConfiguration");
		assertNotNull(config);
	}

	@Test
	public void testCluster2() {
		assertNotNull(ctx);
		assertTrue(ctx.containsBean("yarnCluster"));
		assertTrue(ctx.containsBean("yarnConfiguration"));
		Configuration config = (Configuration) ctx.getBean("yarnConfiguration");
		assertNotNull(config);
	}
	
	@org.springframework.context.annotation.Configuration
	static class Config {
		// just empty to survive without xml configs
	}
	
}
