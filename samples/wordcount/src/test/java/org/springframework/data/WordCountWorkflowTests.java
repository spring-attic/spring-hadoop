package org.springframework.data;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

public class WordCountWorkflowTests {

	@Test
	public void testWorkflowNS() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/launch-context.xml");

		ctx.registerShutdownHook();
		//to keep dev and runtime deployment configuration the same
		System.setProperty("basedir", "src" + File.separator + "main");
		//FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		//fs.delete(new Path("/user/gutenberg/output/word/"), true);

		
		startJobs(ctx);
	}
	
	public void startJobs(ApplicationContext ctx) {
		JobLauncher launcher = ctx.getBean(JobLauncher.class);
		Map<String, Job> jobs = ctx.getBeansOfType(Job.class);

		for (Map.Entry<String, Job> entry : jobs.entrySet()) {
			System.out.println("Executing job " + entry.getKey());
			try {
				if (launcher.run(entry.getValue(), new JobParameters()).getStatus().equals(BatchStatus.FAILED)){
					throw new BeanInitializationException("Failed executing job " + entry.getKey());
				}
			} catch (Exception ex) {
				throw new BeanInitializationException("Cannot execute job " + entry.getKey(), ex);
			}
		}
	}

}
