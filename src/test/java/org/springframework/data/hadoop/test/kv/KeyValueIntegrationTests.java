package org.springframework.data.hadoop.test.kv;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.data.hadoop.mapreduce.JobTemplate;
import org.springframework.data.hadoop.test.word.HadoopSetUp;

import static org.junit.Assert.*;

public class KeyValueIntegrationTests {

	@Rule
	public HadoopSetUp setUp = HadoopSetUp.preferClusterRunning("localhost", 9001);

	private ConfigurableApplicationContext context;

	private JobTemplate jobTemplate;

	@Before
	public void init() throws Exception {
		jobTemplate = new JobTemplate();
		//		if (setUp.isClusterOnline()) {
		//			setUp.setJarFile("target/spring-hadoop-1.0.0.BUILD-SNAPSHOT-test.jar");
		//			jobTemplate.setExtraConfiguration(setUp.getExtraConfiguration());
		//		}
		jobTemplate.setVerbose(true);
		DefaultResourceLoader loader = new DefaultResourceLoader();
		String dest = loader.getResource(".").getURL().toString() + "/input/kv";

		setUp.copy("src/test/resources/input/kv", dest);
		try {
			setUp.delete(context.getResource("output/kv").getURL().toString());
		} catch (Exception ex) {
		}
	}

	@After
	public void close() throws Exception {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void testKeyValueInputFromFile() throws Exception {
		assertTrue(jobTemplate.run("classpath:/jobs/kv/job-context.xml"));
	}

}
