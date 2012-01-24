package org.springframework.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.hadoop.batch.TriggerJobs;

public class WordCountWorkflowTests {

	@Test
	public void testWorkflowNS() throws Exception {
		GenericXmlApplicationContext ctx = new GenericXmlApplicationContext(
				"/launch-context.xml");

		ctx.registerShutdownHook();

		FileSystem fs = FileSystem.get(ctx.getBean(Configuration.class));
		fs.delete(new Path("/ide-test/output/word/"), true);

		TriggerJobs tj = new TriggerJobs();
		tj.startJobs(ctx);
	}

}
