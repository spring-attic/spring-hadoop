package org.springframework.hadoop.test;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.hadoop.JobTemplate;

public class WordCountIntegrationTests {

	@Rule
	public HadoopSetUp setUp = HadoopSetUp.preferClusterRunning("localhost", 9001);

	private ConfigurableApplicationContext context;

	private JobTemplate jobTemplate;

	@Before
	public void init() throws Exception {
		jobTemplate = new JobTemplate();
		if (setUp.isClusterOnline()) {
			setUp.setJarFile("target/spring-hadoop-core-1.0.0.BUILD-SNAPSHOT-test.jar");
			jobTemplate.setExtraConfiguration(setUp.getExtraConfiguration());
			setUp.copy("src/test/resources/input");
		}
		jobTemplate.setVerbose(true);
		setUp.delete("target/output");
	}

	@After
	public void close() throws Exception {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void testNativeJob() throws Exception {
		Configuration conf = setUp.getConfiguration();
		Job job = new Job(conf, "nativeWordCount");
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("src/test/resources/input"));
		FileOutputFormat.setOutputPath(job, new Path("target/output"));
		assertTrue(job.waitForCompletion(true));
	}

	@Test
	public void testAutowiredJob() throws Exception {
		assertTrue(jobTemplate.run("/spring/autowired-job-context.xml"));
	}
	
	public static void main(String[] args) throws Exception {
		new WordCountIntegrationTests().testAutowiredJob();
	}

}
