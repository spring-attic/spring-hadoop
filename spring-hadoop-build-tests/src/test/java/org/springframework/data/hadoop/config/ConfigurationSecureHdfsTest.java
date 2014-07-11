package org.springframework.data.hadoop.config;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.TestUtils;
import org.springframework.data.hadoop.test.tests.Assume;
import org.springframework.data.hadoop.test.tests.TestGroup;
import org.springframework.util.Assert;

/**
 * tests for yarn:configuration secure.
 *
 * @author David Liu
 * @author Janne Valkealahti
 *
 */
public class ConfigurationSecureHdfsTest {

	private static final Log log = LogFactory.getLog(ConfigurationSecureHdfsTest.class);

	@Test
	public void testSecurityConfigWithReadFromKerberizedHdfs() throws Exception {
		// This test is guarded by test group to be run manually
		// until we have a proper way to use secured miniclusters.
		Assume.group(TestGroup.KERBEROS);
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"ConfigurationSecureHdfsTest-context.xml",
				ConfigurationSecureHdfsTest.class);
		Assert.notNull(context.getBean("secureHdfsConfig"));
		Configuration configuration = context.getBean("secureHdfsConfig", Configuration.class);
		Object factory = context.getBean("&secureHdfsConfig");
		String userKeytab = TestUtils.readField("userKeytab", factory);
		assertThat(userKeytab, containsString("keytab"));
		String namenodePrincipal = TestUtils.readField("namenodePrincipal", factory);
		assertThat(namenodePrincipal, containsString("hdfs"));
		String rmPrincipal = TestUtils.readField("rmManagerPrincipal", factory);
		assertThat(rmPrincipal, containsString("yarn"));
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			log.info("fileStatus: " + fileStatus);
		}
		context.close();
	}

}