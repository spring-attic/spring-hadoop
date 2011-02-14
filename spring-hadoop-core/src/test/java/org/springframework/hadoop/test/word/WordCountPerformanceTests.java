package org.springframework.hadoop.test.word;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.hadoop.JobTemplate;
import org.springframework.test.annotation.Repeat;
import org.springframework.util.StopWatch;

public class WordCountPerformanceTests {

	private static Log logger = LogFactory.getLog(WordCountPerformanceTests.class);

	@Rule
	public HadoopSetUp setUp = HadoopSetUp.preferClusterRunning("localhost", 9001);

	@Rule
	// Add System property -Dperformance.test=true to switch on repeat processing
	public RepeatProcessor repeats = new RepeatProcessor(0, !System.getProperty("performance.test", "false").equals(
			"false"));

	private String input = "target/book/input/word";

	private String output = "target/output/word";

	private String zipFile = "src/test/resources/book.zip";

	private ConfigurableApplicationContext context;

	private JobTemplate jobTemplate;

	private StopWatch stopWatch = new StopWatch();

	@Before
	public void init() throws Exception {

		System.setProperty("input.path", input);

		jobTemplate = new JobTemplate();
		jobTemplate.setVerbose(true);
		jobTemplate.addExtraConfiguration("io.sort.mb", "10");

		if (!stopWatch.isRunning()) {
			stopWatch.start();
		}

		setUp.unzip(zipFile, input);
		setUp.delete(output);

		if (setUp.isClusterOnline()) {
			setUp.setJarFile("target/spring-hadoop-core-1.0.0.BUILD-SNAPSHOT-test.jar");
			jobTemplate.setExtraConfiguration(setUp.getExtraConfiguration());
			setUp.copy(input);
		}

	}

	@After
	public void close() throws Exception {
		if (context != null) {
			context.close();
		}
		System.clearProperty("input.path");
		if (stopWatch.isRunning()) {
			stopWatch.stop();
		}
		logger.info(stopWatch);
	}

	@Test
	// @Ignore
	public void warmUp() throws Exception {
		assertTrue(jobTemplate.run(JobConfiguration.class));
	}

	@Test
	@Repeat(10)
	// @Ignore
	public void testVanillaJob() throws Exception {
		assertTrue(jobTemplate.run(VanillaConfiguration.class));
	}

	@Test
	@Repeat(10)
	// @Ignore
	public void testPojoJob() throws Exception {
		assertTrue(jobTemplate.run(PojoConfiguration.class));
	}

	@Test
	@Repeat(10)
	// @Ignore
	public void testStatelessJob() throws Exception {
		assertTrue(jobTemplate.run(StatelessConfiguration.class));
	}

	@Test
	@Repeat(10)
	// @Ignore
	public void testGenericJob() throws Exception {
		assertTrue(jobTemplate.run(GenericConfiguration.class));
	}

}
