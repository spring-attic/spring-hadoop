package org.springframework.data.hadoop.samples;

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/launch-context.xml")
public class WordCountWorkflowTests {

    @Autowired
    private ApplicationContext ctx;

	// hack to make Hadoop FS 'work' on Windows
	private static void hackHadoopStagingOnWin() {
		// do the assignment only on Windows systems
		if (System.getProperty("os.name").startsWith("Windows")) {
			// 0655 = -rwxr-xr-x
			JobSubmissionFiles.JOB_DIR_PERMISSION.fromShort((short) 0655);
			JobSubmissionFiles.JOB_FILE_PERMISSION.fromShort((short) 0655);
		}
	}

	{
		hackHadoopStagingOnWin();
	}

	@Test
	public void testSanityTest() throws Exception {
		assertNotNull(ctx);
	}
}