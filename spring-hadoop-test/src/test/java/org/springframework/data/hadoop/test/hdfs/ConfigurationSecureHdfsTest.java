package org.springframework.data.hadoop.test.hdfs;

import java.io.File;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Assert;

/**
 * tests for yarn:configuration secure login.
 *
 * @author David Liu
 *
 */
public class ConfigurationSecureHdfsTest extends KerberosSecurityTestcase {
	@Test
	public void testAppSubmission() throws Exception {
		MiniKdc kdc = getKdc();
		File workDir = getWorkDir();
		String principal = "foo";
		File keytab = new File(workDir, "foo.keytab");
		kdc.createPrincipal(keytab, principal);

		Set<Principal> principals = new HashSet<Principal>();
		principals.add(new KerberosPrincipal(principal));

		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		org.apache.hadoop.security.SecurityUtil.login(conf,
				keytab.getAbsolutePath(), principal);
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"ConfigurationSecureHdfsTest-context.xml",
				ConfigurationSecureHdfsTest.class);
		System.out.println("---------------------------------------------");
		System.out.println(context.getBean("secureHdfsConfig"));
		Assert.notNull(context.getBean("secureHdfsConfig"));
	}

}